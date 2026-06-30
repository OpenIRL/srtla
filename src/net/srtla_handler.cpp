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

// Accumulate received sequence numbers and emit an SRTLA ACK every RECV_ACK_INT
// packets so the sender can grow its per-link window.
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

// REG1/REG2 are the SRTLA registration handshake. Returns true if the packet
// was a registration packet and has been fully handled.
static bool handle_registration(char (&buf)[MTU], int n, struct sockaddr &srtla_addr, time_t ts) {
  if (is_srtla_reg1(buf, n)) {
    register_group(&srtla_addr, buf, ts);
    return true;
  }
  if (is_srtla_reg2(buf, n)) {
    conn_reg(&srtla_addr, buf, ts);
    return true;
  }
  return false;
}

// When a connection comes back after a timeout, mark it so the cleanup pass
// nurses it back to health with more frequent keepalives.
static void mark_connection_recovery(srtla_conn_group_ptr &g, srtla_conn_ptr &c,
                                     bool was_timed_out, time_t ts) {
  if (c->recovery_start == 0 && was_timed_out) {
    c->recovery_start = ts;
    spdlog::info("[{}:{}] [Group: {}] Connection is recovering",
                print_addr(&c->addr), port_no(&c->addr), static_cast<void *>(g.get()));
  }
}

// Echo SRTLA keepalives straight back to the sender. Returns true if the packet
// was a keepalive and has been handled.
static bool try_echo_keepalive(srtla_conn_group_ptr &g, char (&buf)[MTU], int n,
                               struct sockaddr &srtla_addr) {
  if (!is_srtla_keepalive(buf, n))
    return false;

  int ret = pad_sendto(srtla_sock, &buf, n, 0, &srtla_addr, addr_len);
  if (ret != n) {
    spdlog::error("[{}:{}] [Group: {}] Failed to send SRTLA Keepalive",
                  print_addr(&srtla_addr), port_no(&srtla_addr), static_cast<void *>(g.get()));
  }
  return true;
}

// Per-connection inter-arrival jitter from SRT sender timestamps (RFC 3550).
// Only updated for in-order packets — out-of-order arrivals (common with SRTLA
// multi-path) would produce bogus values from the unsigned timestamp diff.
static void update_jitter(srtla_conn_ptr &c, char (&buf)[MTU]) {
  uint32_t srt_ts;
  std::memcpy(&srt_ts, buf + 8, sizeof(srt_ts));
  srt_ts = be32toh(srt_ts);
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
}

// For SRT data packets: track unique (non-retransmitted) payload, update jitter,
// and feed the sequence number into the SRTLA ACK stream.
static void account_data_packet(srtla_conn_group_ptr &g, srtla_conn_ptr &c,
                                char (&buf)[MTU], int n) {
  int32_t sn = get_srt_sn(buf, n);
  if (sn < 0)
    return;

  // Track unique data bytes (excluding retransmissions)
  if (g->track_data_sn(sn)) {
    c->stats.unique_data_bytes += n;
    c->stats.unique_data_packets++;
  }

  update_jitter(c, buf);

  register_packet(g, c, sn);
}

// An SRT INDUCTION handshake on a group that already has a live session means
// the encoder reconnected: tear down the stale SRT socket and reset stats.
static void handle_encoder_reconnect(srtla_conn_group_ptr &g, char (&buf)[MTU], int n) {
  if (g->srt_sock < 0 || !is_srt_induction(buf, n))
    return;

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

// Lazily open the downstream SRT socket for a group on first traffic. Returns
// false (after removing the group) if the socket could not be established.
static bool ensure_srt_socket(srtla_conn_group_ptr &g) {
  if (g->srt_sock >= 0)
    return true;

  int sock = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
  if (sock < 0) {
    spdlog::error("[Group: {}] Failed to create an SRT socket", static_cast<void *>(g.get()));
    remove_group(g);
    return false;
  }
  g->srt_sock = sock;

  // Set receive buffer size for g->srt_sock
  int bufsize = RECV_BUF_SIZE;
  int ret = setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
  if (ret != 0) {
    spdlog::error("failed to set receive buffer size ({})", bufsize);
    remove_group(g);
    return false;
  }

  // Set send buffer size for g->srt_sock
  int sndbufsize = SEND_BUF_SIZE;
  ret = setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbufsize, sizeof(sndbufsize));
  if (ret != 0) {
    spdlog::error("failed to set send buffer size ({})", bufsize);
    remove_group(g);
    return false;
  }

  // Set g->srt_sock to non-blocking
  int flags = fcntl(sock, F_GETFL, 0);
  if (flags == -1 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1) {
    spdlog::error("failed to set g->srt_sock non-blocking");
    remove_group(g);
    return false;
  }

  ret = connect(sock, &srt_addr, addr_len);
  if (ret != 0) {
    spdlog::error("[Group: {}] Failed to connect() to the SRT socket", static_cast<void *>(g.get()));
    remove_group(g);
    return false;
  }

  uint16_t local_port = get_sock_local_port(sock);
  spdlog::info("[Group: {}] Created SRT socket. Local Port: {}", static_cast<void *>(g.get()), local_port);

  ret = epoll_add(sock, EPOLLIN, g.get());
  if (ret != 0) {
    spdlog::error("[Group: {}] Failed to add the SRT socket to the epoll", static_cast<void *>(g.get()));
    remove_group(g);
    return false;
  }

  // Write file containing association between local port and client IPs
  g->write_socket_info_file();
  return true;
}

// Learn the SRT destination socket ID from forwarded packets; needed to address
// the stats control packets we later send back to the SRT server.
static void learn_dest_socket_id(srtla_conn_group_ptr &g, char (&buf)[MTU], int n) {
  if (g->srt_dest_socket_id == 0 && n >= SRT_MIN_LEN) {
    srt_header_t *hdr = reinterpret_cast<srt_header_t *>(buf);
    uint32_t dest_id = ntohl(hdr->dest_id);
    if (dest_id != 0) {
      g->srt_dest_socket_id = dest_id;
      spdlog::debug("[Group: {}] Learned SRT destination socket ID: {:#x}", static_cast<void *>(g.get()), dest_id);
    }
  }
}

// Forward an SRT packet to the downstream server, removing the group on failure.
static void forward_to_srt(srtla_conn_group_ptr &g, char (&buf)[MTU], int n) {
  int ret = send(g->srt_sock, &buf, n, 0);
  if (ret == n)
    return;

  // Non-blocking backpressure: drop this packet, keep the session alive.
  if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
    spdlog::debug("[Group: {}] SRT send backpressure, dropped a packet", static_cast<void *>(g.get()));
    return;
  }

  spdlog::error("[Group: {}] Failed to forward SRTLA packet, terminating the group", static_cast<void *>(g.get()));
  remove_group(g);
}

static void process_srtla_packet(char (&buf)[MTU], int n, struct sockaddr &srtla_addr, time_t ts) {
  // SRTLA registration handshake (REG1/REG2)
  if (handle_registration(buf, n, srtla_addr, ts))
    return;

  // Only members of a connection group may send anything else
  srtla_conn_group_ptr g;
  srtla_conn_ptr c;
  group_find_by_addr(&srtla_addr, g, c);
  if (!g || !c)
    return;

  bool was_timed_out = conn_timed_out(c, ts);
  c->last_rcvd = ts;
  mark_connection_recovery(g, c, was_timed_out, ts);

  // SRTLA keepalive echo
  if (try_echo_keepalive(g, buf, n, srtla_addr))
    return;

  // Everything below is SRT traffic destined for the downstream server
  if (n < SRT_MIN_LEN) return;

  // Record the most recently active peer and exempt the group from ghost
  // eviction now that it carries real SRT traffic.
  g->last_addr = srtla_addr;
  g->data_seen = true;

  c->stats.bytes_received += n;
  c->stats.packets_received++;

  account_data_packet(g, c, buf, n);

  handle_encoder_reconnect(g, buf, n);

  if (!ensure_srt_socket(g))
    return;

  learn_dest_socket_id(g, buf, n);

  forward_to_srt(g, buf, n);
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
