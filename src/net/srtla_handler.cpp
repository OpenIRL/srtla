/*
    srtla_rec - SRT transport proxy with link aggregation, forked by IRLToolkit
    Copyright (C) 2020-2021 BELABOX project
    Copyright (C) 2024 IRLToolkit Inc.
    Copyright (C) 2024-2026 OpenIRL

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

/* recvmmsg()/sendmmsg() and struct mmsghdr are GNU extensions on glibc */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "net/srtla_handler.h"

#include <cerrno>
#include <cstring>
#include <ctime>

#include <endian.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "core/config.h"
#include "core/conn.h"
#include "core/registry.h"
#include "core/state.h"
#include "net/socket_util.h"
#include "proto/protocol.h"

struct srtla_ack_pkt {
    uint32_t type;
    uint32_t acks[RECV_ACK_INT];
};

static void register_packet(srtla_conn_group_ptr group, srtla_conn_ptr conn, int32_t sn) {
  // store the sequence numbers in BE, as they're transmitted over the network
  conn->recv_log[conn->recv_idx++] = htobe32(sn);

  if (conn->recv_idx == RECV_ACK_INT) {
    srtla_ack_pkt ack;
    ack.type = htobe32(SRTLA_TYPE_ACK << 16);
    std::memcpy(&ack.acks, conn->recv_log.begin(), sizeof(uint32_t) * conn->recv_log.max_size());

    int ret = sendto(srtla_sock, &ack, sizeof(ack), 0, &conn->addr, addr_len);
    if (ret != sizeof(ack)) {
      spdlog::error("[{}:{}] [Group: {}] Failed to send the SRTLA ACK",
          print_addr(&conn->addr), port_no(&conn->addr), static_cast<void *>(group.get()));
    }

    conn->recv_idx = 0;
  }
}

static void process_srtla_packet(char (&buf)[MTU], int n, struct sockaddr &srtla_addr, time_t ts) {
  // Handle srtla registration packets
  if (is_srtla_reg1(buf, n)) {
    register_group(&srtla_addr, buf, ts);
    return;
  }

  if (is_srtla_reg2(buf, n)) {
    conn_reg(&srtla_addr, buf, ts);
    return;
  }

  // Check that the peer is a member of a connection group, discard otherwise
  srtla_conn_group_ptr g;
  srtla_conn_ptr c;
  group_find_by_addr(&srtla_addr, g, c);
  if (!g || !c)
    return;

  // Check if connection was timed out before receiving this packet
  bool was_timed_out = conn_timed_out(c, ts);

  // Update the connection's use timestamp
  c->last_rcvd = ts;

  // For Problem 1: Set recovery_start when the connection is restored
  // When a connection comes back after a timeout, mark it for recovery
  if (c->recovery_start == 0 && was_timed_out) {
    c->recovery_start = ts;
    spdlog::info("[{}:{}] [Group: {}] Connection is recovering",
                print_addr(&c->addr), port_no(&c->addr), static_cast<void *>(g.get()));
  }

  // Resend SRTLA keep-alive packets to the sender
  if (is_srtla_keepalive(buf, n)) {
    int ret = pad_sendto(srtla_sock, &buf, n, 0, &srtla_addr, addr_len);
    if (ret != n) {
      spdlog::error("[{}:{}] [Group: {}] Failed to send SRTLA Keepalive", print_addr(&srtla_addr), port_no(&srtla_addr), static_cast<void *>(g.get()));
    }
    return;
  }

  // Check that the packet is large enough to be an SRT packet, discard otherwise
  if (n < SRT_MIN_LEN) return;

  // Record the most recently active peer
  g->last_addr = srtla_addr;

  // Real SRT traffic: mark the group as no longer a "ghost", so it is exempt
  // from ghost-group eviction under a REG1 flood.
  g->data_seen = true;

  // For Problem 2: Update connection statistics
  c->stats.bytes_received += n;
  c->stats.packets_received++;

  // Keep track of the received data packets to send SRTLA ACKs
  int32_t sn = get_srt_sn(buf, n);
  if (sn >= 0) {
    // Track unique data bytes (excluding retransmissions)
    if (g->track_data_sn(sn)) {
      c->stats.unique_data_bytes += n;
      c->stats.unique_data_packets++;
    }
    // Calculate per-connection jitter from SRT sender timestamps (RFC 3550)
    // Only for in-order packets — out-of-order arrivals (common with SRTLA
    // multi-path) would produce bogus values from the unsigned timestamp diff
    uint32_t srt_ts = be32toh(reinterpret_cast<uint32_t *>(buf)[2]);
    uint64_t arrival_us;
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    arrival_us = static_cast<uint64_t>(tp.tv_sec) * 1000000 + tp.tv_nsec / 1000;

    if (c->stats.last_arrival_us > 0) {
      int32_t ts_diff = static_cast<int32_t>(srt_ts - c->stats.last_srt_timestamp);
      if (ts_diff > 0) {
        // In-order packet: update jitter and reference timestamps
        int64_t d = static_cast<int64_t>(arrival_us - c->stats.last_arrival_us)
                  - static_cast<int64_t>(ts_diff);
        if (d < 0) d = -d;
        // Clamp to 1 second to prevent overflow in EWMA calculation
        if (d > 1000000) d = 1000000;
        // RFC 3550 EWMA: J(i) = J(i-1) + (|D(i-1,i)| - J(i-1)) / 16
        int32_t jdiff = static_cast<int32_t>(d) - static_cast<int32_t>(c->stats.jitter);
        c->stats.jitter = static_cast<uint32_t>(static_cast<int32_t>(c->stats.jitter) + jdiff / 16);

        c->stats.last_srt_timestamp = srt_ts;
        c->stats.last_arrival_us = arrival_us;
      }
      // ts_diff == 0: duplicate packet, ts_diff < 0: out-of-order
      // In both cases, skip jitter update and keep reference point stable
    } else {
      // First data packet on this connection: initialize reference
      c->stats.last_srt_timestamp = srt_ts;
      c->stats.last_arrival_us = arrival_us;
    }

    register_packet(g, c, sn);
  }

  // Detect encoder reconnection: SRT INDUCTION handshake on a group
  // that already has an established SRT session
  if (g->srt_sock >= 0 && is_srt_induction(buf, n)) {
    spdlog::info("[Group: {}] Detected SRT INDUCTION on established session, resetting SRT socket",
                 static_cast<void *>(g.get()));
    g->close_srt_socket();
    g->reset_sn_tracking();

    // Reset connection stats to avoid stale timestamps from the old session
    for (auto &conn : g->conns) {
      uint32_t saved_conn_id = conn->stats.conn_id;
      conn->stats = {};
      conn->stats.conn_id = saved_conn_id;
      conn->recovery_start = 0;
    }
  }

  // Open a connection to the SRT server for the group
  if (g->srt_sock < 0) {
    int sock = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
    if (sock < 0) {
      spdlog::error("[Group: {}] Failed to create an SRT socket", static_cast<void *>(g.get()));
      remove_group(g);
      return;
    }
    g->srt_sock = sock;

    // Set receive buffer size for g->srt_sock
    int bufsize = RECV_BUF_SIZE;
    int ret = setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    if (ret != 0) {
      spdlog::error("failed to set receive buffer size ({})", bufsize);
      remove_group(g);
      return;
    }

    // Set send buffer size for g->srt_sock
    int sndbufsize = SEND_BUF_SIZE;
    ret = setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbufsize, sizeof(sndbufsize));
    if (ret != 0) {
      spdlog::error("failed to set send buffer size ({})", bufsize);
      remove_group(g);
      return;
    }

    // Set g->srt_sock to non-blocking
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1) {
      spdlog::error("failed to set g->srt_sock non-blocking");
      remove_group(g);
      return;
    }

    ret = connect(sock, &srt_addr, addr_len);
    if (ret != 0) {
      spdlog::error("[Group: {}] Failed to connect() to the SRT socket", static_cast<void *>(g.get()));
      remove_group(g);
      return;
    }

    uint16_t local_port = get_sock_local_port(sock);
    spdlog::info("[Group: {}] Created SRT socket. Local Port: {}", static_cast<void *>(g.get()), local_port);

    ret = epoll_add(sock, EPOLLIN, g.get());
    if (ret != 0) {
      spdlog::error("[Group: {}] Failed to add the SRT socket to the epoll", static_cast<void *>(g.get()));
      remove_group(g);
      return;
    }

    // Write file containing association between local port and client IPs
    g->write_socket_info_file();
  }

  // Learn SRT destination socket ID from forwarded packets
  if (g->srt_dest_socket_id == 0 && n >= SRT_MIN_LEN) {
    srt_header_t *hdr = reinterpret_cast<srt_header_t *>(buf);
    uint32_t dest_id = ntohl(hdr->dest_id);
    if (dest_id != 0) {
      g->srt_dest_socket_id = dest_id;
      spdlog::debug("[Group: {}] Learned SRT destination socket ID: {:#x}", static_cast<void *>(g.get()), dest_id);
    }
  }

  int ret = send(g->srt_sock, &buf, n, 0);
  if (ret != n) {
    spdlog::error("[Group: {}] Failed to forward SRTLA packet, terminating the group", static_cast<void *>(g.get()));
    remove_group(g);
  }
}

/*
  Receive a batch of SRTLA packets in a single recvmmsg() syscall and process
  each one. Batching amortizes the per-packet syscall overhead, which matters
  at high bitrate with many bonded connections. Each packet re-resolves its own
  group by source address, so a group removed mid-batch cannot leave a stale
  reference behind.
*/
void handle_srtla_data(time_t ts) {
  struct iovec iovecs[RECV_BATCH_SIZE];
  struct mmsghdr msgs[RECV_BATCH_SIZE];
  char bufs[RECV_BATCH_SIZE][MTU];
  struct sockaddr addrs[RECV_BATCH_SIZE];

  for (int i = 0; i < RECV_BATCH_SIZE; i++) {
    iovecs[i].iov_base = bufs[i];
    iovecs[i].iov_len = MTU;
    msgs[i].msg_hdr = {};
    msgs[i].msg_hdr.msg_name = &addrs[i];
    msgs[i].msg_hdr.msg_namelen = addr_len;
    msgs[i].msg_hdr.msg_iov = &iovecs[i];
    msgs[i].msg_hdr.msg_iovlen = 1;
  }

  int num_msgs = recvmmsg(srtla_sock, msgs, RECV_BATCH_SIZE, MSG_DONTWAIT, nullptr);
  if (num_msgs < 0) {
    if (errno != EAGAIN && errno != EWOULDBLOCK)
      spdlog::error("Failed to read srtla packets: {}", strerror(errno));
    return;
  }

  for (int i = 0; i < num_msgs; i++) {
    int n = static_cast<int>(msgs[i].msg_len);
    if (n > 0)
      process_srtla_packet(bufs[i], n, addrs[i], ts);
  }
}
