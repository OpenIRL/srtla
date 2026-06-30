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

#include "core/registry.h"

#include <algorithm>
#include <cstring>
#include <exception>

#include <endian.h>
#include <sys/socket.h>

#include <spdlog/spdlog.h>

#include "core/config.h"
#include "core/state.h"
#include "net/socket_util.h"
#include "proto/protocol.h"

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

int register_group(struct sockaddr *addr, char *in_buf, time_t ts) {
  // Reject an already-registered remote address before touching the group
  // table, so a re-REG1 can never evict an unrelated pending group.
  srtla_conn_group_ptr group;
  srtla_conn_ptr conn;
  group_find_by_addr(addr, group, conn);
  if (group) {
    srtla_send_reg_err(addr);
    spdlog::error("[{}:{}] Group registration failed: Remote address already registered to group", print_addr(addr), port_no(addr));
    return -1;
  }

  // When the group table is full, try to reclaim a slot from a ghost group
  // (registered but never streamed) before rejecting. This keeps an
  // unauthenticated REG1 flood from locking out the real broadcaster.
  if (conn_groups.size() >= MAX_GROUPS && !evict_oldest_pending_group()) {
    srtla_send_reg_err(addr);
    spdlog::error("[{}:{}] Group registration failed: Max groups reached", print_addr(addr), port_no(addr));
    return -1;
  }

  // Allocate the group
  char *client_id = in_buf + 2;
  try {
    group = std::make_shared<srtla_conn_group>(client_id, ts);
  } catch (const std::exception &e) {
    srtla_send_reg_err(addr);
    spdlog::error("[{}:{}] Group registration failed: {}", print_addr(addr), port_no(addr), e.what());
    return -1;
  }

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
