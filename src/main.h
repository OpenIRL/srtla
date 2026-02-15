/*
    srtla_rec - SRT transport proxy with link aggregation, forked by IRLToolkit
    Copyright (C) 2020-2021 BELABOX project
    Copyright (C) 2024 IRLToolkit Inc.
    Copyright (C) 2024 OpenIRL

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

#pragma once

#include <memory>

#include <spdlog/spdlog.h>

extern "C" {
#include "common.h"
}

#define MAX_CONNS_PER_GROUP 16
#define MAX_GROUPS          200

#define CLEANUP_PERIOD 3
#define GROUP_TIMEOUT  4
#define CONN_TIMEOUT   4

#define KEEPALIVE_PERIOD 1
#define RECOVERY_CHANCE_PERIOD 5

// Constants for connection quality evaluation
#define CONN_QUALITY_EVAL_PERIOD 5 // Evaluation interval in seconds
#define ACK_THROTTLE_INTERVAL 100  // Milliseconds between ACK packets for client control
#define MIN_ACK_RATE 0.2           // Minimum ACK rate (20%) to keep connections alive
#define MIN_ACCEPTABLE_TOTAL_BANDWIDTH_KBPS 1000.0 // Minimum total bandwidth (1 Mbps)
#define GOOD_CONNECTION_THRESHOLD 0.5 // Threshold for "good" connection (50% of max bandwidth)
#define CONNECTION_GRACE_PERIOD 10 // Grace period in seconds before applying penalties
#define WEIGHT_FULL 100
#define WEIGHT_EXCELLENT 85
#define WEIGHT_DEGRADED 70
#define WEIGHT_FAIR 55
#define WEIGHT_POOR 40
#define WEIGHT_CRITICAL 10

#define RECV_ACK_INT 10
#define SEND_BUF_SIZE (100 * 1024 * 1024)
#define RECV_BUF_SIZE (100 * 1024 * 1024)

#define SRT_SOCKET_INFO_PREFIX "/tmp/srtla-group-"

struct connection_stats {
    uint64_t bytes_received;         // Received bytes
    uint64_t packets_received;       // Received packets
    uint32_t bitrate;                // Current bandwidth in kbps (updated every 1s)
    uint64_t last_bw_calc_bytes;     // Bytes at last bandwidth calculation
    uint64_t last_bw_calc_time;      // Timestamp of last bandwidth calculation (ms)
    uint32_t jitter;                 // Smoothed jitter in microseconds (RFC 3550 EWMA)
    uint32_t last_srt_timestamp;     // Last SRT sender timestamp (microseconds)
    uint64_t last_arrival_us;        // Last packet arrival time (microseconds, monotonic)
    // Quality evaluation fields
    uint64_t last_eval_time;         // Last evaluation time (ms)
    uint64_t last_bytes_received;    // Bytes at last evaluation point
    uint32_t error_points;           // Error points
    uint8_t weight_percent;          // Weight in percent (0-100)
    uint64_t last_ack_sent_time;     // Timestamp of last ACK packet (ms)
    double ack_throttle_factor;      // Factor for throttling ACK frequency (0.1-1.0)
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

    // Fields for SRTLA stats reporting
    uint32_t srt_dest_socket_id = 0;     // SRT destination socket ID (learned from forwarded packets)
    uint64_t last_stats_sent_ms = 0;     // Timestamp of last stats packet sent (ms)

    // Fields for load balancing
    uint64_t total_target_bandwidth = 0; // Total bandwidth
    time_t last_quality_eval = 0;        // Last time of quality evaluation
    bool load_balancing_enabled = true;  // Load balancing enabled

    srtla_conn_group(char *client_id, time_t ts);
    ~srtla_conn_group();

    std::vector<struct sockaddr> get_client_addresses();
    void write_socket_info_file();
    void remove_socket_info_file();
    void send_stats_to_srt();

    // Methods for load balancing and connection evaluation
    void evaluate_connection_quality(time_t current_time);
    void adjust_connection_weights(time_t current_time);
};
typedef std::shared_ptr<srtla_conn_group> srtla_conn_group_ptr;

struct srtla_ack_pkt {
    uint32_t type;
    uint32_t acks[RECV_ACK_INT];
};

void send_keepalive(srtla_conn_ptr c, time_t ts);
bool conn_timed_out(srtla_conn_ptr c, time_t ts);

struct conn_bandwidth_info {
    srtla_conn_ptr conn;
    double bandwidth_kbits_per_sec;
};
