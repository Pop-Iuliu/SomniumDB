#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <mutex>
#include <csignal>
#include <chrono>
#include <sched.h>
#include <liburing.h>
#include <poll.h>

#include "metrics.h"
#include "src/core/database.h"
#include "watchdog.h"

using namespace std;

#define PORT 6379
#define URING_ENTRIES 2048

struct Client {
    int fd;
    string buffer;
};

namespace {
    struct [[maybe_unused]] DbValue {
        string data;
        long long expire_at_ms{};
    };
}

static Database db;
static Watchdog watchdog(db);

static long long get_now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static void set_nonblocking(const int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static vector<string> parse_resp(string& buffer) {
    vector<string> args;
    size_t pos = 0;

    if (buffer.empty()) return args;

    if (buffer[0] != '*') {
        buffer.clear();
        return args;
    }

    size_t crlf = buffer.find("\r\n", pos);
    if (crlf == string::npos) return args;

    try {
        const int num_args = stoi(buffer.substr(pos + 1, crlf - pos - 1));
        pos = crlf + 2;

        for (int i = 0; i < num_args; i++) {
            if (pos >= buffer.length()) return {};
            if (buffer[pos] != '$') {
                buffer.clear();
                return {};
            }

            crlf = buffer.find("\r\n", pos);
            if (crlf == string::npos) return {};

            const int len = stoi(buffer.substr(pos + 1, crlf - pos - 1));
            pos = crlf + 2;

            if (pos + len + 2 > buffer.length()) return {};

            args.push_back(buffer.substr(pos, len));
            pos += len + 2;
        }
        buffer.erase(0, pos);
        return args;
    } catch (...) {
        buffer.clear();
        return {};
    }
}

static void adjust_thread_priority(unsigned int current_load) {
    static bool is_high_prio = false;

    if (current_load > 500 && !is_high_prio) {
        sched_param param{};
        param.sched_priority = sched_get_priority_max(SCHED_FIFO);
        if (sched_setscheduler(0, SCHED_FIFO, &param) == 0) {
            is_high_prio = true;
            cout << "[PRIO CLIMB] Thread promovat la SCHED_FIFO (Real-Time) datorita stresului!\n";
        }
    }
    else if (current_load < 100 && is_high_prio) {
        sched_param param{};
        param.sched_priority = 0;
        if (sched_setscheduler(0, SCHED_OTHER, &param) == 0) {
            is_high_prio = false;
            cout << "[PRIO DROP] Trafic normalizat. Revenire la SCHED_OTHER.\n";
        }
    }
}

static void add_poll_request(struct io_uring *ring, Client *client) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_poll_add(sqe, client->fd, POLLIN);
    io_uring_sqe_set_data(sqe, client);
}

int main() {
    start_prometheus_exporter(9090);
    signal(SIGPIPE, SIG_IGN);
    const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    set_nonblocking(server_fd);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    bind(server_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address));
    listen(server_fd, 4096);

    struct io_uring ring;
    if (io_uring_queue_init(URING_ENTRIES, &ring, 0) < 0) {
        cerr << "Eroare la initializarea io_uring. Kernel prea vechi? :( \n";
        return 1;
    }

    auto* server_state = new Client{.fd = server_fd, .buffer = ""};
    add_poll_request(&ring, server_state);
    io_uring_submit(&ring);

    cout << "Server pornit (io_uring) pe portul " << PORT << "...\n";
    watchdog.start();

    while (true) {
        io_uring_submit_and_wait(&ring, 1);

        struct io_uring_cqe *cqe;
        unsigned head;
        unsigned count = 0;

        io_uring_for_each_cqe(&ring, head, cqe) {
            count++;
            auto* current = static_cast<Client*>(io_uring_cqe_get_data(cqe));
            bool keep_client = true;

            if (current->fd == server_fd) {
                sockaddr_in client_addr{};
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &client_len);

                if (client_fd > 0) {
                    set_nonblocking(client_fd);
                    auto* new_client = new Client{client_fd, ""};
                    add_poll_request(&ring, new_client);
                }
            } else {
                char temp_buf[4096];
                int bytes = read(current->fd, temp_buf, sizeof(temp_buf));

                if (bytes <= 0) {
                    db.cleanup_client(current->fd);
                    close(current->fd);
                    delete current;
                    keep_client = false;
                } else {
                    current->buffer.append(temp_buf, bytes);

                    while (true) {
                        vector<string> args = parse_resp(current->buffer);
                        if (args.empty()) break;

                        string response = db.execute(current->fd, args);
                        write(current->fd, response.c_str(), response.length());
                    }
                }
            }
            if (keep_client) {
                add_poll_request(&ring, current);
            }
        }

        io_uring_cq_advance(&ring, count);

        adjust_thread_priority(count);
    }

    io_uring_queue_exit(&ring);
    close(server_fd);
    return 0;
}