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

// Central place for the daemon's tunable constants.

#pragma once

// Group / connection limits
#define MAX_CONNS_PER_GROUP 16
#define MAX_GROUPS          200

// Timeouts and periods (seconds)
#define CLEANUP_PERIOD 3
#define GROUP_TIMEOUT  4
#define CONN_TIMEOUT   4
// Adjustment for Problem 1: Shorter keepalive period for recovery
#define KEEPALIVE_PERIOD 1
#define RECOVERY_CHANCE_PERIOD 5

// Number of sequence numbers accumulated before an SRTLA ACK is sent
#define RECV_ACK_INT 10

// Socket buffer sizes
#define SEND_BUF_SIZE (100 * 1024 * 1024)
#define RECV_BUF_SIZE (100 * 1024 * 1024)

// Event loop / batching
#define MAX_EPOLL_EVENTS 10
// Number of SRTLA packets to receive per recvmmsg() syscall
#define RECV_BATCH_SIZE 64

#define SRT_SOCKET_INFO_PREFIX "/tmp/srtla-group-"
