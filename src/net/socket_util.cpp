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

#include "net/socket_util.h"

#include <cstring>
#include <fstream>
#include <stdexcept>

#include <arpa/inet.h>
#include <endian.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "core/state.h"
#include "proto/protocol.h"

int pad_sendto(int sock, const void *buf, size_t len,
               int flags, const struct sockaddr *addr, socklen_t alen) {
  unsigned char padded[32];
  if (len >= 32) return sendto(sock, buf, len, flags, addr, alen);
  memset(padded, 0, 32);
  memcpy(padded, buf, len);
  int ret = sendto(sock, padded, 32, flags, addr, alen);
  return (ret == 32) ? (int)len : ret;
}

int epoll_add(int fd, uint32_t events, void *priv_data) {
  struct epoll_event ev = {0};
  ev.events = events;
  ev.data.ptr = priv_data;
  return epoll_ctl(socket_epoll, EPOLL_CTL_ADD, fd, &ev);
}

int epoll_rem(int fd) {
  struct epoll_event ev; // non-NULL for Linux < 2.6.9, however unlikely it is
  return epoll_ctl(socket_epoll, EPOLL_CTL_DEL, fd, &ev);
}

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

bool addr_equal(const struct sockaddr *a, const struct sockaddr *b) {
  return memcmp(a, b, addr_len) == 0;
}

std::vector<char> get_random_bytes(size_t size)
{
  std::vector<char> ret;
  ret.resize(size);

  std::ifstream f("/dev/urandom", std::ios::binary);
  f.read(ret.data(), size);
  if (!f || static_cast<size_t>(f.gcount()) != size)
    throw std::runtime_error("get_random_bytes: short read from /dev/urandom");

  return ret;
}

uint16_t get_sock_local_port(int fd)
{
  struct sockaddr_in local_addr = {};
  socklen_t local_addr_len = sizeof(local_addr);
  getsockname(fd, (struct sockaddr *)&local_addr, &local_addr_len);
  return ntohs(local_addr.sin_port);
}

void srtla_send_reg_err(struct sockaddr *addr)
{
  uint16_t header = htobe16(SRTLA_TYPE_REG_ERR);
  pad_sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
}
