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

// The group table: lookup, the SRTLA registration handshake (REG1/REG2/REG3)
// and periodic cleanup of timed-out connections and groups.

#pragma once

#include <ctime>
#include <sys/socket.h>

#include "core/conn.h"

srtla_conn_group_ptr group_find_by_id(char *id);
void group_find_by_addr(struct sockaddr *addr, srtla_conn_group_ptr &rg, srtla_conn_ptr &rc);

int register_group(struct sockaddr *addr, char *in_buf, time_t ts);
void remove_group(srtla_conn_group_ptr group);
bool evict_oldest_pending_group();
int conn_reg(struct sockaddr *addr, char *in_buf, time_t ts);

void cleanup_groups_connections(time_t ts);
