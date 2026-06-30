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

#include "core/conn.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>

#include <arpa/inet.h>
#include <endian.h>
#include <sys/socket.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "core/state.h"
#include "net/socket_util.h"

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

// Signed distance between two SRT data sequence numbers, wrap-safe over the
// 31-bit sequence space (bit 31 is the control flag and is always 0 here).
static inline int32_t srt_sn_diff(int32_t a, int32_t b)
{
  int64_t d = (static_cast<int64_t>(a) - b) & 0x7FFFFFFF;  // 0 .. 2^31-1
  if (d >= 0x40000000) d -= 0x80000000;                    // wrap to signed
  return static_cast<int32_t>(d);
}

bool srtla_conn_group::track_data_sn(int32_t sn)
{
  if (sn_window_base < 0) {
    sn_window.resize(SN_WINDOW_SIZE, false);
    sn_window_base = sn;
  }

  int32_t offset = srt_sn_diff(sn, sn_window_base);

  // Before our window - old packet / retransmission
  if (offset < 0) return false;

  // Beyond window - advance
  if (offset >= SN_WINDOW_SIZE) {
    int32_t new_base = (sn - SN_WINDOW_SIZE / 2) & 0x7FFFFFFF;
    int32_t advance = srt_sn_diff(new_base, sn_window_base);
    if (advance < 0 || advance >= SN_WINDOW_SIZE) {
      std::fill(sn_window.begin(), sn_window.end(), false);
    } else {
      for (int32_t i = 0; i < advance; i++) {
        sn_window[(static_cast<uint32_t>(sn_window_base) + static_cast<uint32_t>(i)) & (SN_WINDOW_SIZE - 1)] = false;
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

// Implementation for Problem 1: Connections with Recovery
void send_keepalive(srtla_conn_ptr c, time_t ts) {
    (void)ts;
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
