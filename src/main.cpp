/*
    srtla - SRT transport proxy with link aggregation
    Copyright (C) 2020-2021 BELABOX project

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

#include "main.h"


int srtla_sock;
struct sockaddr srt_addr;
const socklen_t addr_len = sizeof(struct sockaddr);

std::vector<srtla_conn_group_ptr> conn_groups;

/*

Async I/O support

*/
int socket_epoll;

int epoll_add(int fd, uint32_t events, void *userdata) {
  struct epoll_event ev={0};
  ev.events = events;
  ev.data.ptr = userdata;
  return epoll_ctl(socket_epoll, EPOLL_CTL_ADD, fd, &ev);
}

int epoll_rem(int fd) {
  struct epoll_event ev; // non-NULL for Linux < 2.6.9, however unlikely it is
  return epoll_ctl(socket_epoll, EPOLL_CTL_DEL, fd, &ev);
}


/*

Misc helper functions

*/
void print_help() {
  fprintf(stderr, "Syntax: srtla_rec [-v] SRTLA_LISTEN_PORT SRT_HOST SRT_PORT\n\n-v      Print the version and exit\n");
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

std::vector<char> get_random_bytes(size_t size)
{
  std::vector<char> ret;
  ret.resize(size);

  std::ifstream f("/dev/urandom");
  f.read(ret.data(), size);
  assert(f); // Failed to read fully!
  f.close();

  return ret;
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

int group_find_by_addr(struct sockaddr *addr, srtla_conn_group_ptr &rg, srtla_conn **rc) {
  for (auto &group : conn_groups) {
    for (srtla_conn *c = group->conns; c != NULL; c = c->next) {
      if (const_time_cmp(&(c->addr), addr, addr_len) == 0) {
        rg = group;
        *rc = c;
        return 1;
      }
    }
    if (const_time_cmp(&group->last_addr, addr, addr_len) == 0) {
      rg = group;
      *rc = NULL;
      return 0;
    }
  }

  return -1;
}

srtla_conn_group_ptr group_create(char *client_id, time_t ts) {
  auto server_id = get_random_bytes(SRTLA_ID_LEN / 2); // Generate server ID

  std::array<char, SRTLA_ID_LEN> id;
  std::memcpy(id.begin(), client_id, SRTLA_ID_LEN / 2); // Copy client ID to first half of id buffer
  std::copy(server_id.begin(), server_id.end(), id.begin() + (SRTLA_ID_LEN / 2)); // Copy server ID to last half of id buffer

  // Allocate the new group
  auto g = std::make_shared<srtla_conn_group>();

  // And initialize it with the ID we've built above
  g->id = id;
  g->conns = NULL;
  g->srt_sock = -1;
  g->created_at = ts;

  return g;
}

int group_destroy(srtla_conn_group_ptr g) {
  if (!g)
    return -1;

  for (srtla_conn *c = g->conns; c != NULL;) {
    srtla_conn *next = c->next;
    free(c);
    c = next;
  }

  if (g->srt_sock > 0) {
    epoll_rem(g->srt_sock);
    close(g->srt_sock);
  }

  conn_groups.erase(std::remove(conn_groups.begin(), conn_groups.end(), g), conn_groups.end());

  return 0;
}

int group_count_conns(srtla_conn_group_ptr g) {
  if (!g)
    return 0;

  int count = 0;
  for (srtla_conn *c = g->conns; c != NULL; c = c->next)
    count++;
  return count;
}

int group_reg(struct sockaddr *addr, char *in_buf, time_t ts) {
  srtla_conn_group_ptr g;
  srtla_conn *c;
  int ret;
  uint16_t header;
  char *id;

  if (conn_groups.size() >= MAX_GROUPS) {
    spdlog::error("{}:{}: Group count is {}, rejecting group registration", print_addr(addr), port_no(addr), conn_groups.size());
    goto err;
  }

  // If this remote address is already registered, abort
  ret = group_find_by_addr(addr, g, &c);
  if (ret != -1)
    goto err;

  // Allocate the group
  id = in_buf + 2;
  g = group_create(id, ts);
  if (!g)
    goto err;

  /* Record the address used to register the group
     It won't be allowed to register another group while this one is active */
  g->last_addr = *addr;

  // Build a REG2 packet
  char out_buf[SRTLA_TYPE_REG2_LEN];
  header = htobe16(SRTLA_TYPE_REG2);
  memcpy(out_buf, &header, sizeof(header));
  memcpy(out_buf + sizeof(header), g->id.begin(), SRTLA_ID_LEN);

  // Send the REG2 packet
  ret = sendto(srtla_sock, &out_buf, sizeof(out_buf), 0, addr, addr_len);
  if (ret != sizeof(out_buf))
    goto err;

  spdlog::info("{}:{}: Group {} registered", print_addr(addr), port_no(addr), static_cast<void *>(g.get()));

  conn_groups.push_back(g);

  return 0;

err:
  spdlog::error("{}:{}: Group registration failed", print_addr(addr), port_no(addr));
  header = htobe16(SRTLA_TYPE_REG_ERR);
  sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
  return -1;
}

int conn_reg(struct sockaddr *addr, char *in_buf, time_t ts) {
  srtla_conn_group_ptr g, tmp;
  srtla_conn *c;
  int ret;
  uint16_t header;

  char *id = in_buf + 2;
  g = group_find_by_id(id);
  if (!g) {
    uint16_t header = htobe16(SRTLA_TYPE_REG_NGP);
    sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
    goto err_early;
  }

  /* If the connection is already registered, we'll allow it to register
     again to the same group, but not to a new one */
  ret = group_find_by_addr(addr, tmp, &c);
  if (ret != -1 && tmp != g)
    goto err;

  /* If the connection is already registered to the group, we can
     just skip ahead to sending the SRTLA_REG3 */
  if (ret != 1) {
    int conn_count = group_count_conns(g);
    if (conn_count >= MAX_CONNS_PER_GROUP)
      goto err;

    c = static_cast<srtla_conn *>(malloc(sizeof(srtla_conn)));
    if (!c) {
      spdlog::error("malloc() failed");
      goto err;
    }
    c->addr = *addr;
    c->recv_idx = 0;
    c->last_rcvd = ts;
    c->next = g->conns;
    g->conns = c;
  }

  header = htobe16(SRTLA_TYPE_REG3);
  ret = sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
  if (ret != sizeof(header)) goto err_destroy;

  spdlog::info("{}:{} (group {}): Connection registration", print_addr(addr), port_no(addr), static_cast<void *>(g.get()));

  // If it all worked, mark this peer as the most recently active one
  g->last_addr = *addr;

  return 0;

err_destroy:
  g->conns = c->next;
  free(c);

err:
  header = htobe16(SRTLA_TYPE_REG_ERR);
  sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);

err_early:
  spdlog::error("{}:{}: Connection registration for group {} failed", print_addr(addr), port_no(addr), static_cast<void *>(g.get()));
  return -1;
}

/*

The main network event handlers

Resource limits:
  * connections per group MAX_CONNS_PER_GROUP
  * total groups          MAX_GROUPS

*/

void handle_srt_data(srtla_conn_group_ptr g) {
  char buf[MTU];

  if (!g)
    return;

  int n = recv(g->srt_sock, &buf, MTU, 0);
  if (n < SRT_MIN_LEN) {
    spdlog::error("Group {}: failed to read the SRT sock, terminating the group", static_cast<void *>(g.get()));
    group_destroy(g);
    return;
  }

  // ACK
  if (is_srt_ack(buf, n)) {
    // Broadcast SRT ACKs over all connections for timely delivery
    for (srtla_conn *c = g->conns; c != NULL; c = c->next) {
      int ret = sendto(srtla_sock, &buf, n, 0, &c->addr, addr_len);
      if (ret != n) {
        spdlog::error("{}:{} (Group {}): failed to send the SRT ack", print_addr(&c->addr), port_no(&c->addr), static_cast<void *>(g.get()));
      }
    }
  } else {
    // send other packets over the most recently used SRTLA connection
    int ret = sendto(srtla_sock, &buf, n, 0, &g->last_addr, addr_len);
    if (ret != n) {
      spdlog::error("{}:{} (Group {}): failed to send the SRT packet", print_addr(&g->last_addr), port_no(&g->last_addr), static_cast<void *>(g.get()));
    }
  }
}

void register_packet(srtla_conn_group_ptr g, srtla_conn *c, int32_t sn) {
  // store the sequence numbers in BE, as they're transmitted over the network
  c->recv_log[c->recv_idx++] = htobe32(sn);

  if (c->recv_idx == RECV_ACK_INT) {
    srtla_ack_pkt ack;
    ack.type = htobe32(SRTLA_TYPE_ACK << 16);
    memcpy(&ack.acks, &c->recv_log, sizeof(c->recv_log));

    int ret = sendto(srtla_sock, &ack, sizeof(ack), 0, &c->addr, addr_len);
    if (ret != sizeof(ack)) {
      spdlog::error("{}:{} (Group {}): failed to send the srtla ack", print_addr(&c->addr), port_no(&c->addr), static_cast<void *>(g.get()));
    }

    c->recv_idx = 0;
  }
}

void handle_srtla_data(time_t ts) {
  char buf[MTU];
  int ret;

  // Get the packet
  struct sockaddr srtla_addr;
  socklen_t len = addr_len;
  int n = recvfrom(srtla_sock, &buf, MTU, 0, &srtla_addr, &len);
  if (n < 0) {
    spdlog::error("Failed to read a srtla packet");
    return;
  }

  // Handle srtla registration packets
  if (is_srtla_reg1(buf, n)) {
    group_reg(&srtla_addr, buf, ts);
    return;
  }

  if (is_srtla_reg2(buf, n)) {
    conn_reg(&srtla_addr, buf, ts);
    return;
  }

  // Check that the peer is a member of a connection group, discard otherwise
  srtla_conn *c;
  srtla_conn_group_ptr g;
  ret = group_find_by_addr(&srtla_addr, g, &c);
  if (ret != 1) return;

  // Update the connection's use timestamp
  c->last_rcvd = ts;

  // Resend SRTLA keep-alive packets to the sender
  if (is_srtla_keepalive(buf, n)) {
    int ret = sendto(srtla_sock, &buf, n, 0, &srtla_addr, addr_len);
    if (ret != n) {
      spdlog::error("{}:{} (Group {}): failed to send the srtla keepalive", print_addr(&srtla_addr), port_no(&srtla_addr), static_cast<void *>(g.get()));
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
      spdlog::error("Group {}: failed to create an SRT socket", static_cast<void *>(g.get()));
      group_destroy(g);
      return;
    }
    g->srt_sock = sock;

    int ret = connect(sock, &srt_addr, addr_len);
    if (ret != 0) {
      spdlog::error("Group {}: failed to connect() the SRT socket", static_cast<void *>(g.get()));
      group_destroy(g);
      return;
    }

    ret = epoll_add(sock, EPOLLIN, g.get());
    if (ret != 0) {
      spdlog::error("Group {}: failed to add the SRT socket to the epoll", static_cast<void *>(g.get()));
      group_destroy(g);
      return;
    }
  }

  ret = send(g->srt_sock, &buf, n, 0);
  if (ret != n) {
    spdlog::error("Group {}: failed to forward the srtla packet, terminating the group", static_cast<void *>(g.get()));
    group_destroy(g);
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
void connection_cleanup(time_t ts) {
  static time_t last_ran = 0;
  if ((last_ran + CLEANUP_PERIOD) > ts)
    return;
  last_ran = ts;

  if (!conn_groups.size())
    return;

  int total_groups = conn_groups.size();
  int total_conns = 0;
  int removed_groups = 0;
  int removed_conns = 0;

  spdlog::debug("Started a cleanup run");

  std::vector<srtla_conn_group_ptr> groups_to_remove;

  for (auto &group : conn_groups) {
    srtla_conn *next_c = nullptr;
    srtla_conn **prev_c = &group->conns;

    for (srtla_conn *c = group->conns; c != nullptr; c = next_c) {
      total_conns++;
      next_c = c->next;
      if ((c->last_rcvd + CONN_TIMEOUT) < ts) {
        removed_conns++;
        spdlog::info("{}:{} (Group {}): Connection removed (timed out)", print_addr(&c->addr), port_no(&c->addr), static_cast<void *>(group.get()));
        *prev_c = next_c;
        free(c);
        continue;
      }
      prev_c = &c->next;
    }

    if (!group->conns && (group->created_at + GROUP_TIMEOUT) < ts)
      groups_to_remove.push_back(group);
  }

  removed_groups = groups_to_remove.size();

  for (auto &group : groups_to_remove) {
    conn_groups.erase(std::remove(conn_groups.begin(), conn_groups.end(), group), conn_groups.end());
    group_destroy(group);
    spdlog::info("Group {} removed (no connections)", static_cast<void *>(group.get()));
  }

  spdlog::debug("Clean up run ended. Counted {} groups and {} connections. Removed {} groups and {} connections", total_groups, total_conns, removed_groups, removed_conns);
}

/*
SRT is connection-oriented and it won't reply to our packets at this point
unless we start a handshake, so we do that for each resolved address

Returns: -1 when an error has been encountered
          0 when the address was resolved but SRT appears unreachable
          1 when the address was resolved and SRT appears reachable
*/
int resolve_srt_addr(char *host, char *port) {
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
    spdlog::error("Failed to resolve the address {}:{}", host, port);
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

#define ARG_LISTEN_PORT (argv[1])
#define ARG_SRT_HOST    (argv[2])
#define ARG_SRT_PORT    (argv[3])
int main(int argc, char **argv) {
  // Command line argument parsing
  if (argc == 2 && strcmp(argv[1], "-v") == 0) {
    printf(VERSION "\n");
    exit(0);
  }
  if (argc != 4) {
    print_help();
    exit(0);
  }

  struct sockaddr_in listen_addr;

  int srtla_port = parse_port(ARG_LISTEN_PORT);
  if (srtla_port < 0) {
    print_help();
    exit(0);
  }

  // Try to detect if the SRT server is reachable.
  int ret = resolve_srt_addr(ARG_SRT_HOST, ARG_SRT_PORT);
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
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = INADDR_ANY;
  listen_addr.sin_port = htons(srtla_port);
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

  ret = bind(srtla_sock, (const struct sockaddr *)&listen_addr, addr_len);
  if (ret < 0) {
    spdlog::critical("SRTLA socket bind failed");
    exit(EXIT_FAILURE);
  }

  ret = epoll_add(srtla_sock, EPOLLIN, NULL);
  if (ret != 0) {
    spdlog::critical("Failed to add the srtla sock to the epoll");
    exit(EXIT_FAILURE);
  }

  spdlog::info("irltk_srtla_rec is now running");

  while(true) {
    #define MAX_EPOLL_EVENTS 10
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

    connection_cleanup(ts);
  }
}

