/*
    srtla_rec - SRT transport proxy with link aggregation, forked by IRLToolkit
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
#include <string_view>
#include <chrono>
#include <deque>
#include <numeric>

#include <argparse/argparse.hpp>
#include "main.h"

// Constants
static constexpr size_t MAX_EPOLL_EVENTS = 10;
static constexpr int SOCKET_BUFFER_SIZE = 32 * 1024 * 1024; // 32MB
static constexpr size_t BANDWIDTH_HISTORY_SIZE = 10;
static constexpr double BANDWIDTH_SCALE_FACTOR = 1.2;
static constexpr std::chrono::seconds BANDWIDTH_UPDATE_INTERVAL{1};

// Utility functions implementation
uint16_t get_sock_local_port(int sock) {
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    if (getsockname(sock, (struct sockaddr*)&addr, &len) < 0) {
        return 0;
    }
    return ntohs(addr.sin_port);
}

std::string print_addr(const struct sockaddr* addr) {
    char host[NI_MAXHOST];
    char serv[NI_MAXSERV];

    int ret = getnameinfo(addr, sizeof(struct sockaddr),
                         host, sizeof(host),
                         serv, sizeof(serv),
                         NI_NUMERICHOST | NI_NUMERICSERV);

    if (ret != 0) {
        return std::string();
    }

    return std::string(host) + ":" + serv;
}

int epoll_add(int epoll_fd, int fd, uint32_t events) {
    struct epoll_event ev;
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
}

int epoll_rem(int epoll_fd, int fd) {
    return epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
}

int const_time_cmp(const void *a, const void *b, int len) {
    char diff = 0;
    const char *ca = static_cast<const char*>(a);
    const char *cb = static_cast<const char*>(b);
    for (int i = 0; i < len; i++) {
        diff |= ca[i] - cb[i];
    }
    return diff ? -1 : 0;
}

std::vector<char> get_random_bytes(size_t size) {
    std::vector<char> ret(size);
    std::ifstream f("/dev/urandom");
    f.read(ret.data(), size);
    assert(f);
    f.close();
    return ret;
}

// srtla_conn implementation
srtla_conn::srtla_conn(const struct sockaddr& _addr, time_t ts)
    : addr(_addr), last_rcvd(ts), last_measurement(std::chrono::steady_clock::now()) {
    recv_log.fill(0);
    bandwidth_history.resize(BANDWIDTH_HISTORY_SIZE, 0.0);
}

void srtla_conn::update_bandwidth() {
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_measurement).count();

    if (duration > 0) {
        double current_speed = static_cast<double>(bytes_transferred) / duration;
        bandwidth_history.pop_front();
        bandwidth_history.push_back(current_speed);

        double sum = 0.0;
        double weight_sum = 0.0;
        for (size_t i = 0; i < bandwidth_history.size(); ++i) {
            double weight = i + 1;
            sum += bandwidth_history[i] * weight;
            weight_sum += weight;
        }

        current_bandwidth = sum / weight_sum;
        bytes_transferred = 0;
        last_measurement = now;

        double variance = 0.0;
        double mean = std::accumulate(bandwidth_history.begin(), bandwidth_history.end(), 0.0) /
                     bandwidth_history.size();

        for (const auto& bw : bandwidth_history) {
            variance += std::pow(bw - mean, 2);
        }
        variance /= bandwidth_history.size();

        quality_score = (1.0 / (1.0 + std::sqrt(variance))) * (current_bandwidth / mean);
        quality_score = std::clamp(quality_score, 0.1, 1.0);
    }
}

void srtla_conn::add_bytes(size_t bytes) {
    bytes_transferred += bytes;
}

// srtla_conn_group implementation
srtla_conn_group::srtla_conn_group(const char* client_id, time_t ts, int _epoll_fd)
    : created_at(ts), epoll_fd(_epoll_fd) {
    id.fill(0);
    std::memcpy(id.data(), client_id, SRTLA_ID_LEN / 2);

    auto server_id = get_random_bytes(SRTLA_ID_LEN / 2);
    std::copy(server_id.begin(), server_id.end(), id.begin() + (SRTLA_ID_LEN / 2));
}

srtla_conn_group::~srtla_conn_group() {
    if (srt_sock > 0) {
        remove_socket_info_file();
        epoll_rem(epoll_fd, srt_sock); // Now uses stored epoll_fd
        close(srt_sock);
    }
}

void srtla_conn_group::update_load_balancing() {
    if (conns.empty()) return;

    for (auto& conn : conns) {
        conn->update_bandwidth();
    }

    connection_weights.resize(conns.size());
    double total_weighted_bandwidth = 0.0;

    for (size_t i = 0; i < conns.size(); ++i) {
        double weighted_bandwidth = conns[i]->current_bandwidth * conns[i]->quality_score;
        total_weighted_bandwidth += weighted_bandwidth;
        connection_weights[i] = weighted_bandwidth;
    }

    if (total_weighted_bandwidth > 0) {
        for (auto& weight : connection_weights) {
            weight /= total_weighted_bandwidth;
        }
    } else {
        std::fill(connection_weights.begin(), connection_weights.end(), 1.0 / conns.size());
    }
}

srtla_conn_ptr srtla_conn_group::select_connection_for_packet(size_t packet_size) {
    if (conns.empty()) return nullptr;

    double random_value = static_cast<double>(rand()) / RAND_MAX;
    double cumulative_weight = 0.0;

    for (size_t i = 0; i < conns.size(); ++i) {
        cumulative_weight += connection_weights[i];
        if (random_value <= cumulative_weight) {
            conns[i]->add_bytes(packet_size);
            return conns[i];
        }
    }

    conns.back()->add_bytes(packet_size);
    return conns.back();
}

void srtla_conn_group::write_socket_info_file() {
    if (srt_sock == -1) return;

    uint16_t local_port = get_sock_local_port(srt_sock);
    socket_info_filename = std::string(SRT_SOCKET_INFO_PREFIX) + std::to_string(local_port);

    std::ofstream f(socket_info_filename);
    if (!f) return;

    for (const auto& conn : conns) {
        f << print_addr(&conn->addr) << '\n';
    }
}

void srtla_conn_group::remove_socket_info_file() {
    if (!socket_info_filename.empty()) {
        std::remove(socket_info_filename.c_str());
    }
}

// PacketHandler implementation
int PacketHandler::handle_srt_packet(GlobalContext& ctx, const char* data, size_t len,
                                   const struct sockaddr* addr) {
    if (len < SRTLA_ID_LEN) {
        return -1;
    }

    for (auto& group : ctx.conn_groups) {
        if (const_time_cmp(data, group->id.data(), SRTLA_ID_LEN) == 0) {
            return forward_to_srt(group.get(), data + SRTLA_ID_LEN,
                                len - SRTLA_ID_LEN, addr);
        }
    }

    return create_new_group(ctx, data, len, addr);
}

int PacketHandler::handle_udp_packet(GlobalContext& ctx, srtla_conn_group* group,
                                   const char* data, size_t len) {
    if (!group || len < 1) {
        return -1;
    }

    auto conn = group->select_connection_for_packet(len);
    if (!conn) {
        return -1;
    }

    std::vector<char> packet(SRTLA_ID_LEN + len);
    std::memcpy(packet.data(), group->id.data(), SRTLA_ID_LEN);
    std::memcpy(packet.data() + SRTLA_ID_LEN, data, len);

        return sendto(ctx.srtla_sock, packet.data(), packet.size(), 0,
                     &conn->addr, sizeof(struct sockaddr));
    }

    int PacketHandler::forward_to_srt(srtla_conn_group* group, const char* data,
                                    size_t len, const struct sockaddr* addr) {
        if (!group || !data || len < 1) {
            return -1;
        }

        bool found = false;
        for (auto& conn : group->conns) {
            if (memcmp(&conn->addr, addr, sizeof(struct sockaddr)) == 0) {
                conn->last_rcvd = time(nullptr);
                found = true;
                break;
            }
        }

        if (!found) {
            auto new_conn = std::make_shared<srtla_conn>(*addr, time(nullptr));
            group->conns.push_back(new_conn);
            group->write_socket_info_file();
        }

        return sendto(group->srt_sock, data, len, 0,
                     &group->last_addr, sizeof(struct sockaddr));
    }

    int PacketHandler::create_new_group(GlobalContext& ctx, const char* data,
                                      size_t len, const struct sockaddr* addr) {
        SocketGuard srt_sock(socket(AF_INET, SOCK_DGRAM, 0));
        if (srt_sock.get() < 0) {
            return -1;
        }

        int val = 1;
        if (setsockopt(srt_sock.get(), SOL_SOCKET, SO_REUSEADDR,
                      &val, sizeof(val)) < 0) {
            return -1;
        }

        val = SOCKET_BUFFER_SIZE;
        if (setsockopt(srt_sock.get(), SOL_SOCKET, SO_RCVBUF,
                      &val, sizeof(val)) < 0 ||
            setsockopt(srt_sock.get(), SOL_SOCKET, SO_SNDBUF,
                      &val, sizeof(val)) < 0) {
            return -1;
        }

        struct sockaddr_in bind_addr;
        memset(&bind_addr, 0, sizeof(bind_addr));
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_addr.s_addr = INADDR_ANY;

        if (bind(srt_sock.get(), (struct sockaddr*)&bind_addr,
                sizeof(bind_addr)) < 0) {
            return -1;
        }

        if (epoll_add(ctx.socket_epoll, srt_sock.get(),
                     EPOLLIN | EPOLLERR) < 0) {
            return -1;
        }

        // Create group with epoll_fd
        auto group = std::make_shared<srtla_conn_group>(data, time(nullptr), ctx.socket_epoll);
        group->srt_sock = srt_sock.release();
        group->last_addr = *addr;

        auto initial_conn = std::make_shared<srtla_conn>(*addr, time(nullptr));
        group->conns.push_back(initial_conn);

        ctx.conn_groups.push_back(group);

        return forward_to_srt(group.get(), data + SRTLA_ID_LEN,
                            len - SRTLA_ID_LEN, addr);
    }

    // Event handling implementation
    void handle_event(GlobalContext& ctx, const struct epoll_event& event) {
        char buffer[PacketHandler::MAX_PACKET_SIZE];
        struct sockaddr addr;
        socklen_t addr_len = sizeof(addr);

        if (event.events & EPOLLERR) {
            return;
        }

        if (!(event.events & EPOLLIN)) {
            return;
        }

        if (event.data.fd == ctx.srtla_sock) {
            ssize_t len = recvfrom(ctx.srtla_sock, buffer, sizeof(buffer), 0,
                                  &addr, &addr_len);
            if (len < 0) {
                return;
            }

            PacketHandler::handle_srt_packet(ctx, buffer, len, &addr);
            return;
        }

        for (auto& group : ctx.conn_groups) {
            if (group->srt_sock == event.data.fd) {
                ssize_t len = recvfrom(group->srt_sock, buffer, sizeof(buffer), 0,
                                     &addr, &addr_len);
                if (len < 0) {
                    return;
                }

                group->last_addr = addr;
                PacketHandler::handle_udp_packet(ctx, group.get(), buffer, len);
                return;
            }
        }
    }

    // Connection cleanup implementation
    void cleanup_inactive_connections(GlobalContext& ctx) {
        const time_t now = time(nullptr);
        const time_t timeout = 30; // 30 seconds timeout

        for (auto& group : ctx.conn_groups) {
            auto it = std::remove_if(group->conns.begin(), group->conns.end(),
                [now, timeout](const auto& conn) {
                    return now - conn->last_rcvd > timeout;
                });

            if (it != group->conns.end()) {
                group->conns.erase(it, group->conns.end());
                group->write_socket_info_file();
            }
        }

        auto it = std::remove_if(ctx.conn_groups.begin(), ctx.conn_groups.end(),
            [](const auto& group) {
                return group->conns.empty();
            });

        ctx.conn_groups.erase(it, ctx.conn_groups.end());
    }

    // Server setup implementation
    void parse_arguments(argparse::ArgumentParser& args, int argc, char** argv) {
        args.add_argument("-l", "--listen-port")
            .help("Port to listen for SRTLA connections")
            .required()
            .scan<'i', int>();

        args.add_argument("-b", "--bind-address")
            .help("Address to bind to")
            .default_value(std::string("0.0.0.0"));

        try {
            args.parse_args(argc, argv);
        }
        catch (const std::runtime_error& err) {
            std::cerr << err.what() << std::endl;
            std::cerr << args;
            std::exit(1);
        }
    }

    int setup_srt_server(GlobalContext& ctx, const argparse::ArgumentParser& args) {
        ctx.srtla_sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (ctx.srtla_sock < 0) {
            return -1;
        }

        int val = 1;
        if (setsockopt(ctx.srtla_sock, SOL_SOCKET, SO_REUSEADDR,
                       &val, sizeof(val)) < 0) {
            close(ctx.srtla_sock);
            return -1;
        }

        val = SOCKET_BUFFER_SIZE;
        if (setsockopt(ctx.srtla_sock, SOL_SOCKET, SO_RCVBUF,
                       &val, sizeof(val)) < 0 ||
            setsockopt(ctx.srtla_sock, SOL_SOCKET, SO_SNDBUF,
                       &val, sizeof(val)) < 0) {
            close(ctx.srtla_sock);
            return -1;
        }

        struct sockaddr_in bind_addr;
        memset(&bind_addr, 0, sizeof(bind_addr));
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_port = htons(args.get<int>("listen-port"));

        std::string bind_address = args.get<std::string>("bind-address");
        if (inet_pton(AF_INET, bind_address.c_str(),
                      &bind_addr.sin_addr) <= 0) {
            close(ctx.srtla_sock);
            return -1;
        }

        if (bind(ctx.srtla_sock, (struct sockaddr*)&bind_addr,
                 sizeof(bind_addr)) < 0) {
            close(ctx.srtla_sock);
            return -1;
        }

        ctx.socket_epoll = epoll_create1(0);
        if (ctx.socket_epoll < 0) {
            close(ctx.srtla_sock);
            return -1;
        }

        if (epoll_add(ctx.socket_epoll, ctx.srtla_sock,
                      EPOLLIN | EPOLLERR) < 0) {
            close(ctx.socket_epoll);
            close(ctx.srtla_sock);
            return -1;
        }

        return 0;
    }

    // Main function
    int main(int argc, char **argv) {
        try {
            GlobalContext ctx;
            ctx.last_bandwidth_update = std::chrono::steady_clock::now();

            argparse::ArgumentParser args("srtla_rec");
            parse_arguments(args, argc, argv);

            if (setup_srt_server(ctx, args) < 0) {
                return EXIT_FAILURE;
            }

            struct epoll_event events[MAX_EPOLL_EVENTS];

            while (true) {
                int nfds = epoll_wait(ctx.socket_epoll, events, MAX_EPOLL_EVENTS, 100);

                auto now = std::chrono::steady_clock::now();
                if (now - ctx.last_bandwidth_update >= BANDWIDTH_UPDATE_INTERVAL) {
                    for (auto& group : ctx.conn_groups) {
                        group->update_load_balancing();
                    }
                    ctx.last_bandwidth_update = now;
                }

                if (nfds < 0) {
                    if (errno == EINTR) continue;
                    throw std::runtime_error("epoll_wait failed");
                }

                for (int n = 0; n < nfds; ++n) {
                    handle_event(ctx, events[n]);
                }

                cleanup_inactive_connections(ctx);
            }

            return EXIT_SUCCESS;
        }
        catch (const std::exception& e) {
            spdlog::critical("Fatal error: {}", e.what());
            return EXIT_FAILURE;
        }
    }