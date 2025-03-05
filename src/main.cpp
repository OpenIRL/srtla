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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <endian.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <errno.h>

#include <cstring>
#include <cassert>
#include <vector>
#include <algorithm>
#include <fstream>

#include <argparse/argparse.hpp>

#include "main.h"

int srtla_sock;
struct sockaddr srt_addr;
const socklen_t addr_len = sizeof(struct sockaddr);

std::vector<srtla_conn_group_ptr> conn_groups;

/*
Async I/O support
*/
#define MAX_EPOLL_EVENTS 10

int socket_epoll;

int epoll_add(int fd, uint32_t events, void *priv_data) {
  struct epoll_event ev={0};
  ev.events = events;
  ev.data.ptr = priv_data;
  return epoll_ctl(socket_epoll, EPOLL_CTL_ADD, fd, &ev);
}

int epoll_rem(int fd) {
  struct epoll_event ev; // non-NULL for Linux < 2.6.9, however unlikely it is
  return epoll_ctl(socket_epoll, EPOLL_CTL_DEL, fd, &ev);
}

/*
Misc helper functions
*/
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

inline std::vector<char> get_random_bytes(size_t size)
{
  std::vector<char> ret;
  ret.resize(size);

  std::ifstream f("/dev/urandom");
  f.read(ret.data(), size);
  assert(f); // Failed to read fully!
  f.close();

  return ret;
}

uint16_t get_sock_local_port(int fd)
{
  struct sockaddr_in local_addr = {};
  socklen_t local_addr_len = sizeof(local_addr);
  getsockname(fd, (struct sockaddr *)&local_addr, &local_addr_len);
  return ntohs(local_addr.sin_port);
}

inline void srtla_send_reg_err(struct sockaddr *addr)
{
  uint16_t header = htobe16(SRTLA_TYPE_REG_ERR);
  sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
}

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

srtla_conn_ptr select_best_conn(srtla_conn_group_ptr group) {
  if (!group || group->conns.empty())
    return nullptr;

  // Define what we consider "fair share" threshold (±15% of expected share)
  const double FAIR_SHARE_THRESHOLD = 0.15;

  // Reset usage counters periodically to prevent long-term dominance
  static time_t last_reset = 0;
  time_t current_time;
  get_seconds(&current_time);

  // Track total usage and active connections for load balancing decisions
  uint64_t total_usage = 0;
  // First check if all connections are inactive
  bool all_inactive = true;
  for (auto &conn : group->conns) {
    if (conn->recovery_attempts == 0) {
      all_inactive = false;
      break;
    }
  }
  
  // Reset usage counters every 30 seconds to prevent permanent imbalance
  if (current_time - last_reset > 30) {
    last_reset = current_time;
    for (auto &conn : group->conns) {
      conn->usage_counter = 0;
    }
    spdlog::info("[Group: {}] Reset usage counters to ensure fair distribution", static_cast<void *>(group.get()));
  }

  // Calculate total usage across all active connections
  for (auto &conn : group->conns) {
    if ((conn->last_rcvd + CONN_TIMEOUT) >= current_time)
      total_usage += conn->usage_counter;
  }

  srtla_conn_ptr best_conn = nullptr;
  int active_conn_count = 0;
  int64_t best_score = -1;
  
  for (auto &conn : group->conns) {
    // Don't skip inactive connections entirely - they need a chance to recover
    bool is_active = (conn->last_rcvd + CONN_TIMEOUT) >= current_time;
    
    // Calculate relative freshness (0.0-1.0)
    if (is_active) {
      active_conn_count++;
    }
    
    double freshness = 1.0 - std::min(1.0, static_cast<double>(current_time - conn->last_rcvd) / CONN_TIMEOUT);
    
    // Calculate the expected usage share for perfect distribution
    double expected_share = 1.0 / group->conns.size();
    
    // Calculate actual usage share
    double actual_share = total_usage > 0 ? 
                          static_cast<double>(conn->usage_counter) / total_usage : 
                          0.0;
    
    // Calculate fair share (expected share with tolerance)
    bool is_fair_share = (active_conn_count > 0) && 
                         (fabs(actual_share - expected_share) / expected_share <= FAIR_SHARE_THRESHOLD);
    
    // Calculate distribution factor based on current distribution
    double distribution_factor = 1.0; // Default neutral value
    if (expected_share > 0) {
      // For underutilized connections, give them a boost proportional to how underutilized they are
      if (actual_share < expected_share * (1.0 - FAIR_SHARE_THRESHOLD)) {
        // How far below fair share?
        double shortfall = (expected_share - actual_share) / expected_share;
        // Give bonus based on shortfall, but cap it for new connections
        distribution_factor = std::min(5.0, 1.0 + (shortfall * 5.0));
      }
      // For overutilized connections, reduce their priority proportionally
      else if (actual_share > expected_share * (1.0 + FAIR_SHARE_THRESHOLD)) {
        // How far above fair share?
        double excess = (actual_share - expected_share) / expected_share;
        // Reduce factor based on excess
        distribution_factor = std::max(0.2, 1.0 - (excess * 1.5));
      }
      // If the connection is within fair share threshold, keep neutral factor
    }

    // Give a strong bonus to connections that were recently recovered
    bool recently_recovered = conn->recovery_attempts > 0 && 
                             (current_time - conn->last_rcvd) < 3;
    // But only if they haven't yet reached fair share
    if (recently_recovered && is_fair_share)
      recently_recovered = false;
    
    int64_t recovery_bonus = recently_recovered ? 2000 : 0;
    
    // Calculate final score with more weight on even distribution
    int64_t score = recovery_bonus + 
                    static_cast<int64_t>((distribution_factor * 1500) + 
                                         // Inactive connections get a small penalty
                                         (is_active ? 0 : -500) + 
                                         (freshness * 500)) - 
                                         (conn->recovery_attempts * 200);
    
    if (best_score < 0 || score > best_score) {
      best_conn = conn;
      best_score = score;
    }
  }
  
  // If no good candidate was found and all_inactive is true,
  // still try connections with recovery attempts
  if (!best_conn && !group->conns.empty() && all_inactive) {
    spdlog::debug("[Group: {}] All connections inactive, trying recovery connections", static_cast<void *>(group.get()));
  }
    
  // Log the distribution statistics periodically
  static time_t last_log = 0;
  if (current_time - last_log > 10) {
    last_log = current_time;
    for (auto &conn : group->conns) {
      double percent = total_usage > 0 ? 
                      (double)conn->usage_counter / total_usage * 100.0 : 0.0;
      spdlog::debug("[{}:{}] Usage: {:.1f}% (Expected: {:.1f}%)", print_addr(&conn->addr), 
                   port_no(&conn->addr), percent, (100.0 / group->conns.size()));
    }
  }
    
  // Calculate if rotation is needed based on distribution
  bool needs_rotation = false;
  if (total_usage > 0 && active_conn_count >= 2) {
    double expected_per_conn = 1.0 / active_conn_count;
    for (auto &conn : group->conns) {
      double share = static_cast<double>(conn->usage_counter) / total_usage;
      if (fabs(share - expected_per_conn) / expected_per_conn > FAIR_SHARE_THRESHOLD * 2) {
        needs_rotation = true;
        break;
      }
    }
  }
  static uint64_t rotation_counter = 0;
  rotation_counter++;

  // Every 10th packet, select the least used connection
  if (best_conn && (rotation_counter % 10 == 0)) {
    srtla_conn_ptr least_used = nullptr;
    uint64_t min_usage = UINT64_MAX;
    
    for (auto &conn : group->conns) {
      // Include recently recovered connections regardless of activity
      bool recently_active = (conn->last_rcvd + (CONN_TIMEOUT / 2)) >= current_time;
      
      if (!recently_active && conn->recovery_attempts == 0)
        continue;
      
      if (!needs_rotation && (rotation_counter % 30 != 0))
        continue; // Skip only completely inactive connections
        
      if (conn->usage_counter < min_usage) {
        min_usage = conn->usage_counter;
        least_used = conn;
      }
    }
    
    if (least_used && least_used != best_conn) {
      best_conn = least_used;
      spdlog::debug("[Group: {}] Rotation: switching to least used connection [{}:{}]",
                   static_cast<void *>(group.get()),
                   print_addr(&least_used->addr), port_no(&least_used->addr));
      if (!needs_rotation)
        spdlog::debug("Periodic rotation even though distribution is balanced");
    }
  }
  
  if (best_conn) {
    best_conn->usage_counter++;
    // Reset recovery attempts when connection is used successfully
    if (best_conn->recovery_attempts > 0) {
      best_conn->recovery_attempts = 0;
    }
  }
    
  return best_conn;
}

void group_find_by_addr(struct sockaddr *addr, srtla_conn_group_ptr &rg, srtla_conn_ptr &rc) {
  for (auto &group : conn_groups) {
    for (auto &conn : group->conns) {
      if (const_time_cmp(&(conn->addr), addr, addr_len) == 0) {
        rg = group;
        rc = conn;
        return;
      }
    }
    if (const_time_cmp(&group->last_addr, addr, addr_len) == 0) {
      rg = group;
      rc = nullptr;
      return;
    }
  }
  rg = nullptr;
  rc = nullptr;
}

srtla_conn::srtla_conn(struct sockaddr &_addr, time_t ts) :
  addr(_addr),
  last_rcvd(ts)
{
  recv_log.fill(0);
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

srtla_conn_group::~srtla_conn_group()
{
  conns.clear();

  if (srt_sock > 0) {
    remove_socket_info_file();
    epoll_rem(srt_sock);
    close(srt_sock);
  }
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

int register_group(struct sockaddr *addr, char *in_buf, time_t ts) {
  if (conn_groups.size() >= MAX_GROUPS) {
    srtla_send_reg_err(addr);
    spdlog::error("[{}:{}] Group registration failed: Max groups reached", print_addr(addr), port_no(addr));
    return -1;
  }

  // If this remote address is already registered, abort
  srtla_conn_group_ptr group;
  srtla_conn_ptr conn;
  group_find_by_addr(addr, group, conn);
  if (group) {
    srtla_send_reg_err(addr);
    spdlog::error("[{}:{}] Group registration failed: Remote address already registered to group", print_addr(addr), port_no(addr));
    return -1;
  }

  // Allocate the group
  char *client_id = in_buf + 2;
  group = std::make_shared<srtla_conn_group>(client_id, ts);

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

int conn_reg(struct sockaddr *addr, char *in_buf, time_t ts) {
  char *id = in_buf + 2;
  srtla_conn_group_ptr group = group_find_by_id(id);
  if (!group) {
    uint16_t header = htobe16(SRTLA_TYPE_REG_NGP);
    sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
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
  int ret = sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
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
The main network event handlers
*/
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

  // ACK
  if (is_srt_ack(buf, n)) {
    // Broadcast SRT ACKs over all connections for timely delivery
    for (auto &conn : g->conns) {
      int ret = sendto(srtla_sock, &buf, n, 0, &conn->addr, addr_len);
      if (ret != n)
        spdlog::error("[{}:{}] [Group: {}] Failed to send the SRT ack", print_addr(&conn->addr), port_no(&conn->addr), static_cast<void *>(g.get()));
    }
  } else {
    srtla_conn_ptr best_conn = select_best_conn(g);
    struct sockaddr* target_addr = best_conn ? &best_conn->addr : &g->last_addr;
    
    int ret = sendto(srtla_sock, &buf, n, 0, target_addr, addr_len);
    if (ret != n) {
      spdlog::error("[{}:{}] [Group: {}] Failed to send the SRT packet", 
                    print_addr(target_addr), port_no(target_addr), 
                    static_cast<void *>(g.get()));
    }
  }
}

void register_packet(srtla_conn_group_ptr group, srtla_conn_ptr conn, int32_t sn) {
  // store the sequence numbers in BE, as they're transmitted over the network
  conn->recv_log[conn->recv_idx++] = htobe32(sn);

  if (conn->recv_idx == RECV_ACK_INT) {
    srtla_ack_pkt ack;
    ack.type = htobe32(SRTLA_TYPE_ACK << 16);
    std::memcpy(&ack.acks, conn->recv_log.begin(), sizeof(uint32_t) * conn->recv_log.max_size());

    int ret = sendto(srtla_sock, &ack, sizeof(ack), 0, &conn->addr, addr_len);
    if (ret != sizeof(ack)) {
      spdlog::error("[{}:{}] [Group: {}] Failed to send the SRTLA ACK", print_addr(&conn->addr), port_no(&conn->addr), static_cast<void *>(group.get()));
    }

    conn->recv_idx = 0;
  }
}

void handle_srtla_data(time_t ts) {
  char buf[MTU] = {};

  // Get the packet
  struct sockaddr srtla_addr;
  socklen_t len = addr_len;
  int n = recvfrom(srtla_sock, &buf, MTU, 0, &srtla_addr, &len);
  if (n < 0) {
    spdlog::error("Failed to read an srtla packet");
    return;
  }

  // Handle srtla registration packets
  if (is_srtla_reg1(buf, n)) {
    register_group(&srtla_addr, buf, ts);
    return;
  }

  if (is_srtla_reg2(buf, n)) {
    conn_reg(&srtla_addr, buf, ts);
    return;
  }

  // Check that the peer is a member of a connection group, discard otherwise
  srtla_conn_group_ptr g;
  srtla_conn_ptr c;
  group_find_by_addr(&srtla_addr, g, c);
  if (!g || !c)
    return;

  // Update the connection's use timestamp
  c->last_rcvd = ts;

  // Resend SRTLA keep-alive packets to the sender
  if (is_srtla_keepalive(buf, n)) {
    int ret = sendto(srtla_sock, &buf, n, 0, &srtla_addr, addr_len);
    if (ret != n) {
      spdlog::error("[{}:{}] [Group: {}] Failed to send SRTLA Keepalive", print_addr(&srtla_addr), port_no(&srtla_addr), static_cast<void *>(g.get()));
    }
    return;
  }

  // Check that the packet is large enough to be an SRT packet, discard otherwise
  if (n < SRT_MIN_LEN) return;

  // Record the most recently active peer
  g->last_addr = srtla_addr;

  // Keep track of the received data packets to send SRTLA ACKs
  int32_t sn = get_srt_sn(buf, n);
  if (sn >= 0) {
    register_packet(g, c, sn);
  }

  // Open a connection to the SRT server for the group
  if (g->srt_sock < 0) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
      spdlog::error("[Group: {}] Failed to create an SRT socket", static_cast<void *>(g.get()));
      remove_group(g);
      return;
    }
    g->srt_sock = sock;

    int ret = connect(sock, &srt_addr, addr_len);
    if (ret != 0) {
      spdlog::error("[Group: {}] Failed to connect() to the SRT socket", static_cast<void *>(g.get()));
      remove_group(g);
      return;
    }

    uint16_t local_port = get_sock_local_port(sock);
    spdlog::info("[Group: {}] Created SRT socket. Local Port: {}", static_cast<void *>(g.get()), local_port);

    ret = epoll_add(sock, EPOLLIN, g.get());
    if (ret != 0) {
      spdlog::error("[Group: {}] Failed to add the SRT socket to the epoll", static_cast<void *>(g.get()));
      remove_group(g);
      return;
    }

    // Write file containing association between local port and client IPs
    g->write_socket_info_file();
  }

  int ret = send(g->srt_sock, &buf, n, 0);
  if (ret != n) {
    spdlog::error("[Group: {}] Failed to forward SRTLA packet, terminating the group", static_cast<void *>(g.get()));
    remove_group(g);
  }
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
  int recovered_conns = 0;

  for (std::vector<srtla_conn_group_ptr>::iterator git = conn_groups.begin(); git != conn_groups.end();) {
    auto group = *git;

    size_t before_conns = group->conns.size();
    total_conns += before_conns;
    for (std::vector<srtla_conn_ptr>::iterator cit = group->conns.begin(); cit != group->conns.end();) {
      auto conn = *cit;

      if ((conn->last_rcvd + (CONN_TIMEOUT * 1.5)) < ts) {
        cit = group->conns.erase(cit);
        removed_conns++;
        spdlog::info("[{}:{}] [Group: {}] Connection removed (timed out)", print_addr(&conn->addr), port_no(&conn->addr), static_cast<void *>(group.get()));
      } else {
        // More aggressive recovery logic:
        // 1. Starts earlier with recovery attempts (1/4 of CONN_TIMEOUT)
        // 2. Allows more recovery attempts (5 instead of 3)
        // 3. Sends multiple keepalive packets with each attempt
        bool should_attempt_recovery = 
            (conn->last_rcvd + (CONN_TIMEOUT / 4)) < ts && (conn->recovery_attempts < 5);
        
        if (should_attempt_recovery) {
          uint16_t header = htobe16(SRTLA_TYPE_KEEPALIVE);
          // Send multiple keepalives to increase success probability
          for (int i = 0; i < 3; i++) {
            int ret = sendto(srtla_sock, &header, sizeof(header), 0, &conn->addr, addr_len);
            if (ret == sizeof(header)) {
              recovered_conns++;
            }
          }
          conn->recovery_attempts++;
          spdlog::debug("[{}:{}] [Group: {}] Attempting to recover connection (attempt {}/5)", 
                       print_addr(&conn->addr), port_no(&conn->addr), 
                       static_cast<void *>(group.get()), conn->recovery_attempts);
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

  spdlog::debug("Clean up run ended. Counted {} groups and {} connections. Removed {} groups, {} connections, and attempted to recover {} connections", 
               total_groups, total_conns, removed_groups, removed_conns, recovered_conns);
}

// New function: Proactive ping for connection monitoring
void ping_all_connections(time_t ts) {
  static time_t last_ping = 0;
  // Execute ping every 2 seconds
  if ((last_ping + 2) > ts)
    return;
  last_ping = ts;

  if (conn_groups.empty())
    return;

  // Ping all connections to keep them active
  for (auto &group : conn_groups) {
    for (auto &conn : group->conns) {
      // Send keepalive to all connections that were recently active or
      // are inactive for more than 1/3 of the timeout
      if ((ts - conn->last_rcvd) > (CONN_TIMEOUT / 5)) {
        uint16_t header = htobe16(SRTLA_TYPE_KEEPALIVE);
        sendto(srtla_sock, &header, sizeof(header), 0, &conn->addr, addr_len);
        
        if (conn->recovery_attempts > 0) {
          spdlog::debug("[{}:{}] [Group: {}] Probing inactive connection", 
                      print_addr(&conn->addr), port_no(&conn->addr), static_cast<void *>(group.get()));
        }
      }
      
      // For connections with "recovery_attempts > 0", try even harder to restore them
      if (conn->recovery_attempts > 0) {
        // Send multiple keepalives for recovering connections
        for (int i = 0; i < 2; i++) {
          uint16_t header = htobe16(SRTLA_TYPE_KEEPALIVE);
          sendto(srtla_sock, &header, sizeof(header), 0, &conn->addr, addr_len);
        }
      }
    }
  }
}

/*
SRT is connection-oriented and it won't reply to our packets at this point
unless we start a handshake, so we do that for each resolved address

Returns: -1 when an error has been encountered
          0 when the address was resolved but SRT appears unreachable
          1 when the address was resolved and SRT appears reachable
*/
int resolve_srt_addr(const char *host, const char *port) {
  // Let's set up an SRT handshake induction packet
  srt_handshake_t hs_packet = {0};
  hs_packet.header.type = htobe16(SRT_TYPE_HANDSHAKE);
  hs_packet.version = htobe32(4);
  hs_packet.ext_field = htobe16(2);
  hs_packet.handshake_type = htobe32(1);

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  struct addrinfo *srt_addrs;
  int ret = getaddrinfo(host, port, &hints, &srt_addrs);
  if (ret != 0) {
    spdlog::error("Failed to resolve the address: {}:{}", host, port);
    return -1;
  }

  int tmp_sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (tmp_sock < 0) {
    spdlog::error("Failed to create a UDP socket");
    return -1;
  }

  struct timeval to = { .tv_sec = 1, .tv_usec = 0};
  ret = setsockopt(tmp_sock, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));
  if (ret != 0) {
    spdlog::error("Failed to set a socket timeout");
    return -1;
  }

  int found = -1;
  for (struct addrinfo *addr = srt_addrs; addr != NULL && found == -1; addr = addr->ai_next) {
    spdlog::info("Trying to connect to SRT at {}:{}...", print_addr(addr->ai_addr), port);

    ret = connect(tmp_sock, addr->ai_addr, addr->ai_addrlen);
    if (ret == 0) {
      ret = send(tmp_sock, &hs_packet, sizeof(hs_packet), 0);
      if (ret == sizeof(hs_packet)) {
        char buf[MTU];
        ret = recv(tmp_sock, &buf, MTU, 0);
        if (ret == sizeof(hs_packet)) {
          spdlog::info("Success");
          srt_addr = *addr->ai_addr;
          found = 1;
        }
      } // ret == sizeof(buf)
    } // ret == 0

    if (found == -1) {
      spdlog::info("Error");
    }
  }
  close(tmp_sock);

  if (found == -1) {
    srt_addr = *srt_addrs->ai_addr;
    spdlog::warn("Failed to confirm that a SRT server is reachable at any address. Proceeding with the first address: {}", print_addr(&srt_addr));
    found = 0;
  }

  freeaddrinfo(srt_addrs);

  return found;
}

int main(int argc, char **argv) {
  argparse::ArgumentParser args("srtla_rec", VERSION);

  args.add_argument("--srtla_port").help("Port to bind the SRTLA socket to").default_value((uint16_t)5000).scan<'d', uint16_t>();
  args.add_argument("--srt_hostname").help("Hostname of the downstream SRT server").default_value(std::string{"127.0.0.1"});
  args.add_argument("--srt_port").help("Port of the downstream SRT server").default_value((uint16_t)5001).scan<'d', uint16_t>();
  args.add_argument("--verbose").help("Enable verbose logging").default_value(false).implicit_value(true);

  try {
		args.parse_args(argc, argv);
	} catch (const std::runtime_error& err) {
		std::cerr << err.what() << std::endl;
		std::cerr << args;
		std::exit(1);
	}

  uint16_t srtla_port = args.get<uint16_t>("--srtla_port");
  std::string srt_hostname = args.get<std::string>("--srt_hostname");
  std::string srt_port = std::to_string(args.get<uint16_t>("--srt_port"));

  if (args.get<bool>("--verbose"))
    spdlog::set_level(spdlog::level::debug);

  // Try to detect if the SRT server is reachable.
  int ret = resolve_srt_addr(srt_hostname.c_str(), srt_port.c_str());
  if (ret < 0) {
    exit(EXIT_FAILURE);
  }

  // We use epoll for event-driven network I/O
  socket_epoll = epoll_create(1000); // the number is ignored since Linux 2.6.8
  if (socket_epoll < 0) {
    spdlog::critical("epoll creation failed");
    exit(EXIT_FAILURE);
  }

  // Set up the listener socket for incoming SRT connections
  srtla_sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (srtla_sock < 0) {
    spdlog::critical("SRTLA socket creation failed");
    exit(EXIT_FAILURE);
  }

  // Set receive buffer size to 32MB
  int rcv_buf = 32 * 1024 * 1024;
  ret = setsockopt(srtla_sock, SOL_SOCKET, SO_RCVBUF, &rcv_buf, sizeof(rcv_buf));
  if (ret < 0) {
    spdlog::critical("Failed to set SRTLA socket receive buffer size");
    exit(EXIT_FAILURE);
  }

  // TODO: IPv6 listener
  struct sockaddr_in listen_addr = {};
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = INADDR_ANY;
  listen_addr.sin_port = htons(srtla_port);
  ret = bind(srtla_sock, (const struct sockaddr *)&listen_addr, addr_len);
  if (ret < 0) {
    spdlog::critical("SRTLA socket bind failed");
    exit(EXIT_FAILURE);
  }

  ret = epoll_add(srtla_sock, EPOLLIN, NULL);
  if (ret != 0) {
    spdlog::critical("Failed to add the SRTLA sock to the epoll");
    exit(EXIT_FAILURE);
  }

  spdlog::info("srtla_rec is now running");

  while(true) {
    struct epoll_event events[MAX_EPOLL_EVENTS];
    int eventcnt = epoll_wait(socket_epoll, events, MAX_EPOLL_EVENTS, 1000);

    time_t ts = 0;
    int ret = get_seconds(&ts);
    if (ret != 0)
      spdlog::error("Failed to get the current time");

    size_t group_cnt;
    for (int i = 0; i < eventcnt; i++) {
      group_cnt = conn_groups.size();
      if (events[i].data.ptr == NULL) {
        handle_srtla_data(ts);
      } else {
        auto g = static_cast<srtla_conn_group *>(events[i].data.ptr);
        handle_srt_data(group_find_by_id(g->id.data()));
      }

      /* If we've removed a group due to a socket error, then we might have
         pending events already waiting for us in events[], and now pointing
         to freed() memory. Get an updated list from epoll_wait() */
      if (conn_groups.size() < group_cnt)
        break;
    } // for

    cleanup_groups_connections(ts);
    ping_all_connections(ts);
  }
}

