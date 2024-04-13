#pragma once

#include <memory>

#define MAX_CONNS_PER_GROUP 8
#define MAX_GROUPS          200

#define CLEANUP_PERIOD 3
#define GROUP_TIMEOUT  10
#define CONN_TIMEOUT   10

#define RECV_ACK_INT 10

struct srtla_conn {
  struct srtla_conn *next = nullptr;
  struct sockaddr addr = {};
  time_t last_rcvd;
  int recv_idx;
  uint32_t recv_log[RECV_ACK_INT];
};

typedef std::shared_ptr<srtla_conn> srtla_conn_ptr;

struct srtla_conn_group {
  srtla_conn *conns = nullptr;
  time_t created_at;
  int srt_sock;
  struct sockaddr last_addr;
  char id[SRTLA_ID_LEN];
};

typedef std::shared_ptr<srtla_conn_group> srtla_conn_group_ptr;

struct srtla_ack_pkt {
  uint32_t type;
  uint32_t acks[RECV_ACK_INT];
};
