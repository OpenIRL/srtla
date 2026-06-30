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

// The SRTLA connection / group domain model: a group bundles the bonded
// connections of one broadcaster and owns the downstream SRT socket.

#pragma once

#include <array>
#include <cstdint>
#include <ctime>
#include <memory>
#include <vector>
#include <sys/socket.h>

#include "core/config.h"
#include "proto/protocol.h"

struct connection_stats {
    uint64_t bytes_received;         // Total received bytes (including retransmissions)
    uint64_t packets_received;       // Total received packets
    uint64_t unique_data_bytes;      // Unique data bytes (excluding retransmissions)
    uint64_t unique_data_packets;    // Unique data packets (excluding retransmissions)
    uint32_t bitrate;                // Usable SRT payload bitrate in kbps (unique data, no headers)
    uint32_t throughput;             // Total network throughput in kbps (including retransmissions)
    uint64_t last_bw_calc_bytes;     // Total bytes at last bandwidth calculation
    uint64_t last_bw_calc_unique;    // Unique bytes at last bandwidth calculation
    uint64_t last_bw_calc_unique_pkts; // Unique packets at last bandwidth calculation
    uint64_t last_bw_calc_time;      // Timestamp of last bandwidth calculation (ms)
    uint32_t jitter;                 // Smoothed jitter in microseconds (RFC 3550 EWMA)
    uint32_t last_srt_timestamp;     // Last SRT sender timestamp (microseconds)
    uint64_t last_arrival_us;        // Last packet arrival time (microseconds, monotonic)
    uint32_t conn_id;                // Anonymous connection ID (FNV-1a hash of IP:port)
};

struct srtla_conn {
    struct sockaddr addr = {};
    time_t last_rcvd = 0;
    int recv_idx = 0;
    std::array<uint32_t, RECV_ACK_INT> recv_log;

    // Fields for connection quality evaluation
    connection_stats stats = {};
    time_t recovery_start = 0; // Time when the connection began to recover
    time_t connection_start = 0; // Time when the connection was established

    srtla_conn(struct sockaddr &_addr, time_t ts);
};
typedef std::shared_ptr<srtla_conn> srtla_conn_ptr;

struct srtla_conn_group {
    std::array<char, SRTLA_ID_LEN> id;
    std::vector<srtla_conn_ptr> conns;
    time_t created_at = 0;
    int srt_sock = -1;
    struct sockaddr last_addr = {};

    // True once the group has forwarded a real SRT packet; a group that never
    // does is a "ghost" (e.g. from a REG1 flood) and may be evicted under load.
    bool data_seen = false;

    // Fields for SRTLA stats reporting
    uint32_t srt_dest_socket_id = 0;     // SRT destination socket ID (learned from forwarded packets)
    uint64_t last_stats_sent_ms = 0;     // Timestamp of last stats packet sent (ms)

    // Sequence number tracking for retransmission detection
    static constexpr int32_t SN_WINDOW_SIZE = 65536;
    std::vector<bool> sn_window;
    int32_t sn_window_base = -1;
    bool track_data_sn(int32_t sn);      // Returns true if SN is new (not a retransmission)
    void reset_sn_tracking();

    srtla_conn_group(char *client_id, time_t ts);
    ~srtla_conn_group();
    void close_srt_socket();

    std::vector<struct sockaddr> get_client_addresses();
    void write_socket_info_file();
    void remove_socket_info_file();
    void send_stats_to_srt();
};
typedef std::shared_ptr<srtla_conn_group> srtla_conn_group_ptr;

void send_keepalive(srtla_conn_ptr c, time_t ts);
bool conn_timed_out(srtla_conn_ptr c, time_t ts);
