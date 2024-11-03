#ifndef SRTLA_MAIN_H
#define SRTLA_MAIN_H

#include <memory>
#include <vector>
#include <array>
#include <chrono>
#include <string>
#include <deque>
#include <sys/socket.h>
#include <spdlog/spdlog.h>

// Constants
static constexpr size_t RECV_ACK_INT = 32;              // Size of receive acknowledgment interval
static constexpr size_t SRTLA_ID_LEN = 32;             // Length of SRTLA identifier
static constexpr char SRT_SOCKET_INFO_PREFIX[] = "/tmp/srt_socket_"; // Prefix for socket info files

// RAII Socket Guard class
class SocketGuard {
private:
    int fd_;
public:
    explicit SocketGuard(int fd) : fd_(fd) {}
    ~SocketGuard() { if(fd_ >= 0) close(fd_); }
    int get() const { return fd_; }
    int release() { int tmp = fd_; fd_ = -1; return tmp; }

    // Prevent copying
    SocketGuard(const SocketGuard&) = delete;
    SocketGuard& operator=(const SocketGuard&) = delete;
};

// Forward declarations
class srtla_conn;
class srtla_conn_group;

// Smart pointer type definitions
using srtla_conn_ptr = std::shared_ptr<srtla_conn>;
using srtla_conn_group_ptr = std::shared_ptr<srtla_conn_group>;

// Global context structure declaration
struct GlobalContext {
    int srtla_sock;
    struct sockaddr srt_addr;
    const socklen_t addr_len{sizeof(struct sockaddr)};
    int socket_epoll;
    std::vector<srtla_conn_group_ptr> conn_groups;
    std::chrono::steady_clock::time_point last_bandwidth_update;
};

// Utility functions declarations
std::vector<char> get_random_bytes(size_t len);
int const_time_cmp(const void *a, const void *b, int len);
uint16_t get_sock_local_port(int sock);
std::string print_addr(const struct sockaddr* addr);
int epoll_add(int epoll_fd, int fd, uint32_t events);
int epoll_rem(int epoll_fd, int fd);

// Connection class declaration
class srtla_conn {
public:
    struct sockaddr addr;                    // Remote address
    time_t last_rcvd;                       // Last received packet timestamp
    std::array<uint32_t, RECV_ACK_INT> recv_log; // Receive log
    size_t recv_idx{0};                     // Current receive log index

    std::deque<double> bandwidth_history;    // Bandwidth history
    double current_bandwidth{0.0};           // Current bandwidth
    uint64_t bytes_transferred{0};           // Bytes transferred since last measurement
    std::chrono::steady_clock::time_point last_measurement; // Last bandwidth measurement timestamp
    double quality_score{1.0};              // Connection quality score

    srtla_conn(const struct sockaddr& _addr, time_t ts);
    void update_bandwidth();
    void add_bytes(size_t bytes);
};

// Connection group class declaration
class srtla_conn_group {
private:
    std::string socket_info_filename;
    std::vector<double> connection_weights;
    int epoll_fd;

public:
    std::array<char, SRTLA_ID_LEN> id;      // Group identifier
    struct sockaddr last_addr;               // Last remote address
    std::vector<srtla_conn_ptr> conns;      // Active connections
    int srt_sock{-1};                       // SRT socket
    const time_t created_at;                 // Creation timestamp

    explicit srtla_conn_group(const char* client_id, time_t ts, int _epoll_fd);
    ~srtla_conn_group();

    void update_load_balancing();
    srtla_conn_ptr select_connection_for_packet(size_t packet_size);
    void write_socket_info_file();
    void remove_socket_info_file();
};

// Packet handler class declaration
class PacketHandler {
public:
    static constexpr size_t MAX_PACKET_SIZE = 1500;

    static int handle_srt_packet(GlobalContext& ctx, const char* data, size_t len,
                               const struct sockaddr* addr);
    static int handle_udp_packet(GlobalContext& ctx, srtla_conn_group* group,
                               const char* data, size_t len);

private:
    static int forward_to_srt(srtla_conn_group* group, const char* data,
                            size_t len, const struct sockaddr* addr);
    static int create_new_group(GlobalContext& ctx, const char* data,
                              size_t len, const struct sockaddr* addr);
};

// Function declarations
void handle_event(GlobalContext& ctx, const struct epoll_event& event);
void cleanup_inactive_connections(GlobalContext& ctx);
void parse_arguments(argparse::ArgumentParser& args, int argc, char** argv);
int setup_srt_server(GlobalContext& ctx, const argparse::ArgumentParser& args);

#endif // SRTLA_MAIN_H