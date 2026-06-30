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

// Stateless socket / epoll helpers. The epoll-touching ones operate on the
// shared socket_epoll fd from core/state.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <sys/socket.h>

/* Pad small sendto() to 32 bytes to avoid carrier NAT drops on 2-byte packets */
int pad_sendto(int sock, const void *buf, size_t len,
               int flags, const struct sockaddr *addr, socklen_t alen);

int epoll_add(int fd, uint32_t events, void *priv_data);
int epoll_rem(int fd);

// Constant-time compare for the secret group ID.
int const_time_cmp(const void *a, const void *b, int len);

// Fast (early-exit) equality for peer addresses, which are not secrets.
bool addr_equal(const struct sockaddr *a, const struct sockaddr *b);

std::vector<char> get_random_bytes(size_t size);
uint16_t get_sock_local_port(int fd);

void srtla_send_reg_err(struct sockaddr *addr);
