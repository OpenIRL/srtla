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

#include "net/srt_handler.h"

#include <cerrno>
#include <cstring>

#include <sys/socket.h>
#include <sys/uio.h>

#include <spdlog/spdlog.h>

#include "core/config.h"
#include "core/state.h"
#include "core/registry.h"
#include "proto/protocol.h"

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
