/*
    irltk_srtla_rec - SRT transport proxy with link aggregation, forked by IRLToolkit
    Copyright (C) 2020-2021 BELABOX project
    Copyright (C) 2024 IRLToolkit Inc.

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
#define GROUP_TIMEOUT  10
#define CONN_TIMEOUT   10

#define RECV_ACK_INT 10

#define SRT_SOCKET_INFO_PREFIX "/tmp/srtla-group-"

struct srtla_conn {
    struct sockaddr addr = {};
    time_t last_rcvd = 0;
    int recv_idx = 0;
    std::array<uint32_t, RECV_ACK_INT> recv_log;

    srtla_conn(struct sockaddr &_addr, time_t ts);
};
typedef std::shared_ptr<srtla_conn> srtla_conn_ptr;

struct srtla_conn_group {
    std::array<char, SRTLA_ID_LEN> id;
    std::vector<srtla_conn_ptr> conns;
    time_t created_at = 0;
    int srt_sock = -1;
    struct sockaddr last_addr = {};

    srtla_conn_group(char *client_id, time_t ts);
    ~srtla_conn_group();

    std::vector<struct sockaddr> get_client_addresses();
    void write_socket_info_file();
    void remove_socket_info_file();
};
typedef std::shared_ptr<srtla_conn_group> srtla_conn_group_ptr;

struct srtla_ack_pkt {
    uint32_t type;
    uint32_t acks[RECV_ACK_INT];
};
