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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <endian.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>

#include <cstring>
#include <cassert>
#include <vector>
#include <algorithm>
#include <fstream>
#include <chrono>

#include <argparse/argparse.hpp>

#include "main.h"

int srtla_sock;
struct sockaddr srt_addr;
const socklen_t addr_len = sizeof(struct sockaddr);

/* Pad small sendto() to 32 bytes to avoid carrier NAT drops on 2-byte packets */
static inline int pad_sendto(int sock, const void *buf, size_t len,
                             int flags, const struct sockaddr *addr, socklen_t alen) {
  unsigned char padded[32];
  if (len >= 32) return sendto(sock, buf, len, flags, addr, alen);
  memset(padded, 0, 32);
  memcpy(padded, buf, len);
  int ret = sendto(sock, padded, 32, flags, addr, alen);
  return (ret == 32) ? (int)len : ret;
}

std::vector<srtla_conn_group_ptr> conn_groups;

/*
Async I/O support
*/
#define MAX_EPOLL_EVENTS 10

/* Number of SRTLA packets to receive per recvmmsg() syscall */
#define RECV_BATCH_SIZE 64

int socket_epoll;

int epoll_add(int fd, uint32_t events, void *priv_data) {
  struct epoll_event ev={0};
  ev.events = events;
  ev.data.ptr = priv_data;
  return epoll_ctl(socket_epoll, EPOLL_CTL_ADD, fd, &ev);
}

int epoll_rem(int fd) {
  struct epoll_event ev; // non-NULL for Linux < 2.6.9, however unlikely it is
  return epoll_ctl(socket_epoll, EPOLL_CTL_DEL, fd, &ev);
}

/*
Misc helper functions
*/
int const_time_cmp(const void *a, const void *b, int len) {
  char diff = 0;
  char *ca = (char *)a;
  char *cb = (char *)b;
  for (int i = 0; i < len; i++) {
    diff |= *ca - *cb;
    ca++;
    cb++;
  }

  return diff ? -1 : 0;
}

/* Fast equality for peer addresses. Unlike const_time_cmp (used for the secret
   group ID), addresses are not secrets, so an early-exit memcmp is both correct
   and faster in the per-packet lookup hot path. */
static inline bool addr_equal(const struct sockaddr *a, const struct sockaddr *b) {
  return memcmp(a, b, addr_len) == 0;
}

inline std::vector<char> get_random_bytes(size_t size)
{
  std::vector<char> ret;
  ret.resize(size);

  std::ifstream f("/dev/urandom");
  f.read(ret.data(), size);
  assert(f); // Failed to read fully!
  f.close();

  return ret;
}

uint16_t get_sock_local_port(int fd)
{
  struct sockaddr_in local_addr = {};
  socklen_t local_addr_len = sizeof(local_addr);
  getsockname(fd, (struct sockaddr *)&local_addr, &local_addr_len);
  return ntohs(local_addr.sin_port);
}

inline void srtla_send_reg_err(struct sockaddr *addr)
{
  uint16_t header = htobe16(SRTLA_TYPE_REG_ERR);
  pad_sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
}

/* Defined after remove_group() below */
bool evict_oldest_pending_group();

/*
Connection and group management functions
*/
srtla_conn_group_ptr group_find_by_id(char *id) {
  for (auto &group : conn_groups) {
    if (const_time_cmp(group->id.begin(), id, SRTLA_ID_LEN) == 0)
      return group;
  }
  return nullptr;
}

void group_find_by_addr(struct sockaddr *addr, srtla_conn_group_ptr &rg, srtla_conn_ptr &rc) {
  for (auto &group : conn_groups) {
    for (auto &conn : group->conns) {
      if (addr_equal(&conn->addr, addr)) {
        rg = group;
        rc = conn;
        return;
      }
    }
    if (addr_equal(&group->last_addr, addr)) {
      rg = group;
      rc = nullptr;
      return;
    }
  }
  rg = nullptr;
  rc = nullptr;
}

/*
  FNV-1a hash for generating anonymous connection IDs from IP:port
*/
static uint32_t fnv1a_hash(const void *data, size_t len) {
  uint32_t hash = 0x811c9dc5;
  const uint8_t *bytes = static_cast<const uint8_t *>(data);
  for (size_t i = 0; i < len; i++) {
    hash ^= bytes[i];
    hash *= 0x01000193;
  }
  return hash;
}

srtla_conn::srtla_conn(struct sockaddr &_addr, time_t ts) :
  addr(_addr),
  last_rcvd(ts)
{
  recv_log.fill(0);

  // Initialize statistics
  stats.bytes_received = 0;
  stats.packets_received = 0;

  // Connection ID: FNV-1a hash of sin_addr + sin_port
  struct sockaddr_in *sin = reinterpret_cast<struct sockaddr_in *>(&addr);
  uint8_t hash_input[6]; // 4 bytes addr + 2 bytes port
  memcpy(hash_input, &sin->sin_addr.s_addr, 4);
  memcpy(hash_input + 4, &sin->sin_port, 2);
  stats.conn_id = fnv1a_hash(hash_input, sizeof(hash_input));

  recovery_start = 0;
  connection_start = ts;
}

srtla_conn_group::srtla_conn_group(char *client_id, time_t ts) :
  created_at(ts)
{
  id.fill(0);

  // Copy client ID to first half of id buffer
  std::memcpy(id.begin(), client_id, SRTLA_ID_LEN / 2);

  // Generate server ID, then copy to last half of id buffer
  auto server_id = get_random_bytes(SRTLA_ID_LEN / 2); 
  std::copy(server_id.begin(), server_id.end(), id.begin() + (SRTLA_ID_LEN / 2));
}

void srtla_conn_group::close_srt_socket()
{
  if (srt_sock < 0)
    return;

  // Send SRT SHUTDOWN to cleanly end the session on the SRT server
  if (srt_dest_socket_id != 0) {
    srt_header_t shutdown_pkt = {};
    shutdown_pkt.type = htobe16(SRT_TYPE_SHUTDOWN);
    shutdown_pkt.dest_id = htonl(srt_dest_socket_id);
    send(srt_sock, &shutdown_pkt, sizeof(shutdown_pkt), 0);
    spdlog::info("[Group: {}] Sent SRT SHUTDOWN to backend", static_cast<void *>(this));
  }

  remove_socket_info_file();
  epoll_rem(srt_sock);
  close(srt_sock);
  srt_sock = -1;
  srt_dest_socket_id = 0;
  last_stats_sent_ms = 0;
}

bool srtla_conn_group::track_data_sn(int32_t sn)
{
  if (sn_window_base < 0) {
    sn_window.resize(SN_WINDOW_SIZE, false);
    sn_window_base = sn;
  }

  int32_t offset = sn - sn_window_base;

  // Before our window - old packet / retransmission
  if (offset < 0) return false;

  // Beyond window - advance
  if (offset >= SN_WINDOW_SIZE) {
    int32_t new_base = sn - SN_WINDOW_SIZE / 2;
    int32_t advance = new_base - sn_window_base;
    if (advance >= SN_WINDOW_SIZE) {
      std::fill(sn_window.begin(), sn_window.end(), false);
    } else {
      for (int32_t i = 0; i < advance; i++) {
        sn_window[(sn_window_base + i) & (SN_WINDOW_SIZE - 1)] = false;
      }
    }
    sn_window_base = new_base;
  }

  int idx = sn & (SN_WINDOW_SIZE - 1);
  if (sn_window[idx]) return false;
  sn_window[idx] = true;
  return true;
}

void srtla_conn_group::reset_sn_tracking()
{
  sn_window_base = -1;
  sn_window.clear();
}

srtla_conn_group::~srtla_conn_group()
{
  conns.clear();
  close_srt_socket();
}

std::vector<struct sockaddr> srtla_conn_group::get_client_addresses()
{
  std::vector<struct sockaddr> ret;
  for (auto conn : conns)
    ret.emplace_back(conn->addr);
  return ret;
}

void srtla_conn_group::write_socket_info_file()
{
  if (srt_sock == -1)
    return;

  uint16_t local_port = get_sock_local_port(srt_sock);
  std::string file_name = std::string(SRT_SOCKET_INFO_PREFIX) + std::to_string(local_port);

  auto client_addresses = get_client_addresses();

  std::ofstream f(file_name);
  for (auto &addr : client_addresses)
    f << print_addr(&addr) << std::endl;
  f.close();

  spdlog::debug("[Group: {}] Wrote SRTLA socket info file", static_cast<void *>(this));
}

void srtla_conn_group::remove_socket_info_file()
{
  if (srt_sock == -1)
    return;

  uint16_t local_port = get_sock_local_port(srt_sock);
  std::string file_name = std::string(SRT_SOCKET_INFO_PREFIX) + std::to_string(local_port);

  std::remove(file_name.c_str());
}

int register_group(struct sockaddr *addr, char *in_buf, time_t ts) {
  // When the group table is full, try to reclaim a slot from a ghost group
  // (registered but never streamed) before rejecting. This keeps an
  // unauthenticated REG1 flood from locking out the real broadcaster.
  if (conn_groups.size() >= MAX_GROUPS && !evict_oldest_pending_group()) {
    srtla_send_reg_err(addr);
    spdlog::error("[{}:{}] Group registration failed: Max groups reached", print_addr(addr), port_no(addr));
    return -1;
  }

  // If this remote address is already registered, abort
  srtla_conn_group_ptr group;
  srtla_conn_ptr conn;
  group_find_by_addr(addr, group, conn);
  if (group) {
    srtla_send_reg_err(addr);
    spdlog::error("[{}:{}] Group registration failed: Remote address already registered to group", print_addr(addr), port_no(addr));
    return -1;
  }

  // Allocate the group
  char *client_id = in_buf + 2;
  group = std::make_shared<srtla_conn_group>(client_id, ts);

  /* Record the address used to register the group
     It won't be allowed to register another group while this one is active */
  group->last_addr = *addr;

  // Build a REG2 packet
  char out_buf[SRTLA_TYPE_REG2_LEN];
  uint16_t header = htobe16(SRTLA_TYPE_REG2);
  std::memcpy(out_buf, &header, sizeof(header));
  std::memcpy(out_buf + sizeof(header), group->id.begin(), SRTLA_ID_LEN);

  // Send the REG2 packet
  int ret = sendto(srtla_sock, &out_buf, sizeof(out_buf), 0, addr, addr_len);
  if (ret != sizeof(out_buf)) {
    spdlog::error("[{}:{}] Group registration failed: Send error", print_addr(addr), port_no(addr));
    return -1;
  }

  conn_groups.push_back(group);

  spdlog::info("[{}:{}] [Group: {}] Group registered", print_addr(addr), port_no(addr), static_cast<void *>(group.get()));
  return 0;
}

void remove_group(srtla_conn_group_ptr group)
{
  if (!group)
    return;

  conn_groups.erase(std::remove(conn_groups.begin(), conn_groups.end(), group), conn_groups.end());

  group.reset();
}

/* Reclaim a slot from the oldest "ghost" group — one with no connections that
   has never carried real SRT traffic. Never touches a streaming group. Returns
   true if one was evicted. */
bool evict_oldest_pending_group() {
  srtla_conn_group_ptr oldest;
  for (auto &group : conn_groups) {
    if (!group->conns.empty() || group->data_seen)
      continue;
    if (!oldest || group->created_at < oldest->created_at)
      oldest = group;
  }

  if (!oldest)
    return false;

  spdlog::warn("[Group: {}] Evicting pending group to admit new registration (group table full)", static_cast<void *>(oldest.get()));
  remove_group(oldest);
  return true;
}

int conn_reg(struct sockaddr *addr, char *in_buf, time_t ts) {
  char *id = in_buf + 2;
  srtla_conn_group_ptr group = group_find_by_id(id);
  if (!group) {
    uint16_t header = htobe16(SRTLA_TYPE_REG_NGP);
    pad_sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
    spdlog::error("[{}:{}] Connection registration failed: No group found", print_addr(addr), port_no(addr));
    return -1;
  }

  /* If the connection is already registered, we'll allow it to register
     again to the same group, but not to a new one */
  srtla_conn_group_ptr tmp;
  srtla_conn_ptr conn;
  group_find_by_addr(addr, tmp, conn);
  if (tmp && tmp != group) {
    srtla_send_reg_err(addr);
    spdlog::error("[{}:{}] [Group: {}] Connection registration failed: Provided group ID mismatch", print_addr(addr), port_no(addr), static_cast<void *>(group.get()));
    return -1;
  }

  /* If the connection is already registered to the group, we can
     just skip ahead to sending the SRTLA_REG3 */
  bool already_registered = true;
  if (!conn) {
    if (group->conns.size() >= MAX_CONNS_PER_GROUP) {
      srtla_send_reg_err(addr);
      spdlog::error("[{}:{}] [Group: {}] Connection registration failed: Max group conns reached", print_addr(addr), port_no(addr), static_cast<void *>(group.get()));
      return -1;
    }

    conn = std::make_shared<srtla_conn>(*addr, ts);
    already_registered = false;
  }

  uint16_t header = htobe16(SRTLA_TYPE_REG3);
  int ret = pad_sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
  if (ret != sizeof(header)) {
    spdlog::error("[{}:{}] [Group: {}] Connection registration failed: Socket send error", print_addr(addr), port_no(addr), static_cast<void *>(group.get()));
    return -1;
  }

  if (!already_registered)
    group->conns.push_back(conn);

  group->write_socket_info_file();

  // If it all worked, mark this peer as the most recently active one
  group->last_addr = *addr;

  spdlog::info("[{}:{}] [Group: {}] Connection registration", print_addr(addr), port_no(addr), static_cast<void *>(group.get()));
  return 0;
}

/*
The main network event handlers
*/
void handle_srt_data(srtla_conn_group_ptr g) {
  char buf[MTU];

  if (!g)
    return;

  int n = recv(g->srt_sock, &buf, MTU, 0);
  if (n < SRT_MIN_LEN) {
    spdlog::error("[Group: {}] Failed to read the SRT sock, terminating the group", static_cast<void *>(g.get()));
    remove_group(g);
    return;
  }

  // Broadcast SRT ACKs and NAKs over all connections for timely delivery
  if (is_srt_ack(buf, n) || is_srt_nak(buf, n)) {
    // Send to every connection in a single sendmmsg() syscall
    struct mmsghdr msgs[MAX_CONNS_PER_GROUP];
    struct iovec iovecs[MAX_CONNS_PER_GROUP];
    unsigned int cnt = 0;
    for (auto &conn : g->conns) {
      if (cnt >= MAX_CONNS_PER_GROUP) break;
      iovecs[cnt].iov_base = buf;
      iovecs[cnt].iov_len = n;
      msgs[cnt].msg_hdr = {};
      msgs[cnt].msg_hdr.msg_name = &conn->addr;
      msgs[cnt].msg_hdr.msg_namelen = addr_len;
      msgs[cnt].msg_hdr.msg_iov = &iovecs[cnt];
      msgs[cnt].msg_hdr.msg_iovlen = 1;
      cnt++;
    }
    if (cnt > 0) {
      int sent = sendmmsg(srtla_sock, msgs, cnt, 0);
      if (sent < 0) {
        spdlog::error("[Group: {}] Failed to broadcast the SRT packet: {}", static_cast<void *>(g.get()), strerror(errno));
      } else if (static_cast<unsigned int>(sent) < cnt) {
        spdlog::warn("[Group: {}] Broadcast sent only {}/{} messages", static_cast<void *>(g.get()), sent, cnt);
      }
    }
  } else {
    // send other packets over the most recently used SRTLA connection
    int ret = sendto(srtla_sock, &buf, n, 0, &g->last_addr, addr_len);
    if (ret != n) {
      spdlog::error("[{}:{}] [Group: {}] Failed to send the SRT packet", print_addr(&g->last_addr), port_no(&g->last_addr), static_cast<void *>(g.get()));
    }
  }
}

void register_packet(srtla_conn_group_ptr group, srtla_conn_ptr conn, int32_t sn) {
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

/*
  Send SRTLA per-connection stats to the SRT server as a custom control packet.
  Called from the main loop for each group, throttled to once per second.
*/
void srtla_conn_group::send_stats_to_srt() {
  // Guards: need socket ID, valid socket, and at least one connection
  if (srt_dest_socket_id == 0 || srt_sock < 0 || conns.empty())
    return;

  uint64_t now_ms = 0;
  if (get_ms(&now_ms) != 0)
    return;

  // Throttle to once per second
  if (last_stats_sent_ms > 0 && (now_ms - last_stats_sent_ms) < 1000)
    return;

  last_stats_sent_ms = now_ms;

  uint8_t num_peers = static_cast<uint8_t>(std::min(conns.size(), static_cast<size_t>(16)));

  // Calculate per-connection bitrate (unique payload) and throughput (total network load)
  uint32_t total_bw_kbps = 0;
  uint32_t total_throughput_kbps = 0;
  for (auto &conn : conns) {
    if (conn->stats.last_bw_calc_time > 0) {
      uint64_t delta_ms = now_ms - conn->stats.last_bw_calc_time;
      if (delta_ms > 0) {
        // Bitrate: unique payload only (subtract 16-byte SRT header per packet)
        uint64_t unique_bytes = conn->stats.unique_data_bytes - conn->stats.last_bw_calc_unique;
        uint64_t unique_pkts = conn->stats.unique_data_packets - conn->stats.last_bw_calc_unique_pkts;
        uint64_t payload_bytes = unique_bytes - unique_pkts * SRT_MIN_LEN;
        conn->stats.bitrate = static_cast<uint32_t>((payload_bytes * 8) / delta_ms);
        // Throughput: total bytes on the wire
        uint64_t bytes_diff = conn->stats.bytes_received - conn->stats.last_bw_calc_bytes;
        conn->stats.throughput = static_cast<uint32_t>((bytes_diff * 8) / delta_ms);
      }
    }
    conn->stats.last_bw_calc_unique = conn->stats.unique_data_bytes;
    conn->stats.last_bw_calc_unique_pkts = conn->stats.unique_data_packets;
    conn->stats.last_bw_calc_bytes = conn->stats.bytes_received;
    conn->stats.last_bw_calc_time = now_ms;
    total_bw_kbps += conn->stats.bitrate;
    total_throughput_kbps += conn->stats.throughput;
  }

  // Build packet: SRT header (4 words) + Stats header (4 words) + Per-peer (7 words each)
  // Max: 4 + 4 + 16*7 = 120 words = 480 bytes
  uint32_t buf[4 + 4 + 16 * 7];
  memset(buf, 0, sizeof(buf));

  // SRT Control Packet Header (16 bytes, network byte order)
  buf[0] = htobe32(static_cast<uint32_t>(SRTLA_TYPE_STATS) << 16); // Control bit + Type
  buf[1] = htobe32(0);               // Reserved
  buf[2] = htobe32(0);               // Timestamp
  buf[3] = htobe32(srt_dest_socket_id);

  // Stats Header (16 bytes)
  uint32_t word0 = (static_cast<uint32_t>(1) << 24) |             // version = 1
                   (static_cast<uint32_t>(num_peers) << 16);       // num_peers
  buf[4] = htobe32(word0);
  buf[5] = htobe32(total_bw_kbps);
  buf[6] = htobe32(static_cast<uint32_t>(now_ms >> 32));           // timestamp_high
  buf[7] = htobe32(static_cast<uint32_t>(now_ms & 0xFFFFFFFF));   // timestamp_low

  // Per-Peer entries (28 bytes each)
  for (uint8_t i = 0; i < num_peers; i++) {
    auto &conn = conns[i];

    uint32_t uptime_s = (conn->connection_start > 0) ?
        static_cast<uint32_t>(now_ms / 1000 - conn->connection_start) : 0;

    buf[8 + i * 7 + 0] = htobe32(conn->stats.conn_id);
    buf[8 + i * 7 + 1] = htobe32(conn->stats.bitrate);
    buf[8 + i * 7 + 2] = htobe32(conn->stats.jitter);
    buf[8 + i * 7 + 3] = htobe32(static_cast<uint32_t>(conn->stats.bytes_received >> 32));
    buf[8 + i * 7 + 4] = htobe32(static_cast<uint32_t>(conn->stats.bytes_received & 0xFFFFFFFF));
    buf[8 + i * 7 + 5] = htobe32(uptime_s);
    buf[8 + i * 7 + 6] = htobe32(conn->stats.throughput);
  }

  size_t pkt_size = (4 + 4 + num_peers * 7) * sizeof(uint32_t);
  int ret = send(srt_sock, buf, pkt_size, 0);
  if (ret < 0) {
    spdlog::debug("[Group: {}] Failed to send SRTLA stats packet: {}", static_cast<void *>(this), strerror(errno));
  } else {
    spdlog::debug("[Group: {}] Sent SRTLA stats packet ({} peers, {} kbps bitrate, {} kbps throughput)",
                  static_cast<void *>(this), num_peers, total_bw_kbps, total_throughput_kbps);
  }
}

/*
  Freeing resources

  Groups:
    * new groups with no connection: created_at < (ts - G_TIMEOUT)
    * other groups: when all connections have timed out
  Connections:
    * GC last_rcvd < (ts - CONN_TIMEOUT)
*/
void cleanup_groups_connections(time_t ts) {
  static time_t last_ran = 0;
  if ((last_ran + CLEANUP_PERIOD) > ts)
    return;
  last_ran = ts;

  if (!conn_groups.size())
    return;

  spdlog::debug("Starting a cleanup run...");

  int total_groups = conn_groups.size();
  int total_conns = 0;
  int removed_groups = 0;
  int removed_conns = 0;

  for (std::vector<srtla_conn_group_ptr>::iterator git = conn_groups.begin(); git != conn_groups.end();) {
    auto group = *git;

    size_t before_conns = group->conns.size();
    total_conns += before_conns;
    for (std::vector<srtla_conn_ptr>::iterator cit = group->conns.begin(); cit != group->conns.end();) {
      auto conn = *cit;

      // Check if the connection is in recovery mode
      if (conn->recovery_start > 0) {
        // If the connection has received data since recovery started, it's recovering
        if (conn->last_rcvd > conn->recovery_start) {
          if ((ts - conn->recovery_start) > RECOVERY_CHANCE_PERIOD) {
            spdlog::info("[{}:{}] [Group: {}] Connection recovery completed", 
                       print_addr(&conn->addr), port_no(&conn->addr), static_cast<void *>(group.get()));
            conn->recovery_start = 0;
          } else {
            // Send keepalive packets more frequently during the recovery phase
            if ((conn->last_rcvd + KEEPALIVE_PERIOD) < ts) {
              send_keepalive(conn, ts);
            }
          }
        } 
        // If the recovery phase takes too long without success, give up
        else if ((conn->recovery_start + RECOVERY_CHANCE_PERIOD) < ts) {
          spdlog::info("[{}:{}] [Group: {}] Connection recovery failed", 
                     print_addr(&conn->addr), port_no(&conn->addr), static_cast<void *>(group.get()));
          conn->recovery_start = 0;
        }
      }

      if ((conn->last_rcvd + CONN_TIMEOUT) < ts) {
        cit = group->conns.erase(cit);
        removed_conns++;
        spdlog::info("[{}:{}] [Group: {}] Connection removed (timed out)", print_addr(&conn->addr), port_no(&conn->addr), static_cast<void *>(group.get()));
      } else {
        // Send keepalive packets to connections more frequently if they are in recovery mode
        if (conn->recovery_start > 0 && (conn->last_rcvd + KEEPALIVE_PERIOD) < ts) {
          send_keepalive(conn, ts);
        }
        cit++;
      }
    }

    if (!group->conns.size() && (group->created_at + GROUP_TIMEOUT) < ts) {
      git = conn_groups.erase(git);
      removed_groups++;
      spdlog::info("[Group: {}] Group removed (no connections)", static_cast<void *>(group.get()));
    } else {
      if (before_conns != group->conns.size())
        group->write_socket_info_file();
      git++;
    }
  }

  spdlog::debug("Clean up run ended. Counted {} groups and {} connections. Removed {} groups and {} connections", total_groups, total_conns, removed_groups, removed_conns);
}

/*
SRT is connection-oriented and it won't reply to our packets at this point
unless we start a handshake, so we do that for each resolved address

Returns: -1 when an error has been encountered
          0 when the address was resolved but SRT appears unreachable
          1 when the address was resolved and SRT appears reachable
*/
int resolve_srt_addr(const char *host, const char *port) {
  // Let's set up an SRT handshake induction packet
  srt_handshake_t hs_packet = {0};
  hs_packet.header.type = htobe16(SRT_TYPE_HANDSHAKE);
  hs_packet.version = htobe32(4);
  hs_packet.ext_field = htobe16(2);
  hs_packet.handshake_type = htobe32(1);

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  struct addrinfo *srt_addrs;
  int ret = getaddrinfo(host, port, &hints, &srt_addrs);
  if (ret != 0) {
    spdlog::error("Failed to resolve the address: {}:{}", host, port);
    return -1;
  }

  int tmp_sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (tmp_sock < 0) {
    spdlog::error("Failed to create a UDP socket");
    return -1;
  }

  // Set receive buffer size for tmp_sock
  int bufsize = RECV_BUF_SIZE;
  ret = setsockopt(tmp_sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
  if (ret != 0) {
    spdlog::error("Failed to set a receive buffer size ({} bytes)", bufsize);
    return -1;
  }

  // Set send buffer size for tmp_sock
  bufsize = SEND_BUF_SIZE;
  ret = setsockopt(tmp_sock, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
  if (ret != 0) {
    spdlog::error("Failed to set a send buffer size ({} bytes)", bufsize);
    return -1;
  }

  struct timeval to = { .tv_sec = 1, .tv_usec = 0};
  ret = setsockopt(tmp_sock, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));
  if (ret != 0) {
    spdlog::error("Failed to set a socket timeout");
    return -1;
  }

  int found = -1;
  for (struct addrinfo *addr = srt_addrs; addr != NULL && found == -1; addr = addr->ai_next) {
    spdlog::info("Trying to connect to SRT at {}:{}...", print_addr(addr->ai_addr), port);

    ret = connect(tmp_sock, addr->ai_addr, addr->ai_addrlen);
    if (ret == 0) {
      ret = send(tmp_sock, &hs_packet, sizeof(hs_packet), 0);
      if (ret == sizeof(hs_packet)) {
        char buf[MTU];
        ret = recv(tmp_sock, &buf, MTU, 0);
        if (ret == sizeof(hs_packet)) {
          spdlog::info("Success");
          srt_addr = *addr->ai_addr;
          found = 1;
        }
      } // ret == sizeof(buf)
    } // ret == 0

    if (found == -1) {
      spdlog::info("Error");
    }
  }
  close(tmp_sock);

  if (found == -1) {
    srt_addr = *srt_addrs->ai_addr;
    spdlog::warn("Failed to confirm that a SRT server is reachable at any address. Proceeding with the first address: {}", print_addr(&srt_addr));
    found = 0;
  }

  freeaddrinfo(srt_addrs);

  return found;
}

// Implementation for Problem 1: Connections with Recovery
void send_keepalive(srtla_conn_ptr c, time_t ts) {
    uint16_t pkt = htobe16(SRTLA_TYPE_KEEPALIVE);
    int ret = pad_sendto(srtla_sock, &pkt, sizeof(pkt), 0, &c->addr, addr_len);
    
    if (ret != sizeof(pkt)) {
        spdlog::error("[{}:{}] Failed to send keepalive packet",
            print_addr(&c->addr), port_no(&c->addr));
    } else {
        spdlog::debug("[{}:{}] Sent keepalive packet",
            print_addr(&c->addr), port_no(&c->addr));
    }
}

bool conn_timed_out(srtla_conn_ptr c, time_t ts) {
  return (c->last_rcvd + CONN_TIMEOUT) < ts;
}

int main(int argc, char **argv) {
  argparse::ArgumentParser args("srtla_rec", VERSION);

  args.add_argument("--srtla_port").help("Port to bind the SRTLA socket to").default_value((uint16_t)5000).scan<'d', uint16_t>();
  args.add_argument("--srt_hostname").help("Hostname of the downstream SRT server").default_value(std::string{"127.0.0.1"});
  args.add_argument("--srt_port").help("Port of the downstream SRT server").default_value((uint16_t)4001).scan<'d', uint16_t>();
  args.add_argument("--log_level").help("Set logging level (trace, debug, info, warn, error, critical)").default_value(std::string{"info"});

  try {
		args.parse_args(argc, argv);
	} catch (const std::runtime_error& err) {
		std::cerr << err.what() << std::endl;
		std::cerr << args;
		std::exit(1);
	}

  uint16_t srtla_port = args.get<uint16_t>("--srtla_port");
  std::string srt_hostname = args.get<std::string>("--srt_hostname");
  std::string srt_port = std::to_string(args.get<uint16_t>("--srt_port"));
  std::string log_level = args.get<std::string>("--log_level");

  // Set log level based on the provided argument
  if (log_level == "trace") {
    spdlog::set_level(spdlog::level::trace);
  } else if (log_level == "debug") {
    spdlog::set_level(spdlog::level::debug);
  } else if (log_level == "info") {
    spdlog::set_level(spdlog::level::info);
  } else if (log_level == "warn") {
    spdlog::set_level(spdlog::level::warn);
  } else if (log_level == "error") {
    spdlog::set_level(spdlog::level::err);
  } else if (log_level == "critical") {
    spdlog::set_level(spdlog::level::critical);
  } else {
    spdlog::warn("Invalid log level '{}' specified, using 'info' as default", log_level);
    spdlog::set_level(spdlog::level::info);
  }

  // Try to detect if the SRT server is reachable.
  int ret = resolve_srt_addr(srt_hostname.c_str(), srt_port.c_str());
  if (ret < 0) {
    exit(EXIT_FAILURE);
  }

  // We use epoll for event-driven network I/O
  socket_epoll = epoll_create(1000); // the number is ignored since Linux 2.6.8
  if (socket_epoll < 0) {
    spdlog::critical("epoll creation failed");
    exit(EXIT_FAILURE);
  }

  // Set up the listener socket for incoming SRT connections
  srtla_sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (srtla_sock < 0) {
    spdlog::critical("SRTLA socket creation failed");
    exit(EXIT_FAILURE);
  }

  // Set receive buffer size for srtla_sock
  int bufsize = RECV_BUF_SIZE;
  ret = setsockopt(srtla_sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
  if (ret != 0) {
    spdlog::error("failed to set receive buffer size ({})", bufsize);
    exit(EXIT_FAILURE);
  }

  // Set send buffer size for srtla_sock
  bufsize = SEND_BUF_SIZE;
  ret = setsockopt(srtla_sock, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
  if (ret != 0) {
    spdlog::error("failed to set send buffer size ({})", bufsize);
    exit(EXIT_FAILURE);
  }

  // Set srtla_sock to non-blocking
  int flags = fcntl(srtla_sock, F_GETFL, 0);
  if (flags == -1 || fcntl(srtla_sock, F_SETFL, flags | O_NONBLOCK) == -1) {
    spdlog::error("failed to set srtla_sock non-blocking");
    exit(EXIT_FAILURE);
  }

  // TODO: IPv6 listener
  struct sockaddr_in listen_addr = {};
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = INADDR_ANY;
  listen_addr.sin_port = htons(srtla_port);
  ret = bind(srtla_sock, (const struct sockaddr *)&listen_addr, addr_len);
  if (ret < 0) {
    spdlog::critical("SRTLA socket bind failed");
    exit(EXIT_FAILURE);
  }

  ret = epoll_add(srtla_sock, EPOLLIN, NULL);
  if (ret != 0) {
    spdlog::critical("Failed to add the SRTLA sock to the epoll");
    exit(EXIT_FAILURE);
  }

  spdlog::info("srtla_rec is now running");

  while(true) {
    struct epoll_event events[MAX_EPOLL_EVENTS];
    int eventcnt = epoll_wait(socket_epoll, events, MAX_EPOLL_EVENTS, 1000);

    time_t ts = 0;
    int ret = get_seconds(&ts);
    if (ret != 0)
      spdlog::error("Failed to get the current time");

    size_t group_cnt;
    for (int i = 0; i < eventcnt; i++) {
      group_cnt = conn_groups.size();
      if (events[i].data.ptr == NULL) {
        handle_srtla_data(ts);
      } else {
        auto g = static_cast<srtla_conn_group *>(events[i].data.ptr);
        handle_srt_data(group_find_by_id(g->id.data()));
      }

      /* If we've removed a group due to a socket error, then we might have
         pending events already waiting for us in events[], and now pointing
         to freed() memory. Get an updated list from epoll_wait() */
      if (conn_groups.size() < group_cnt)
        break;
    } // for

    // Send SRTLA stats to SRT server for each group (throttled internally to 1/s)
    for (auto &g : conn_groups) {
      g->send_stats_to_srt();
    }

    cleanup_groups_connections(ts);
  }
}

