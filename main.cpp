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
#include <sys/epoll.h>

using namespace std;

#define MAX_EVENTS 10
#define PORT 6379

struct Client {
    int fd;
    string buffer;
};

#include <chrono>
#include "database.h"

auto server_start_time = std::chrono::steady_clock::now();
long long total_commands = 0;

namespace {
    struct [[maybe_unused]] DbValue {
        string data;
        long long expire_at_ms{}; // 0 - traieste vesnic, ms pana cand moare
    };
}

static Database db;

// get time
static long long get_now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static vector<string> parse_resp(string& buffer) {
    vector<string> args;
    size_t pos = 0;

    if (buffer.empty() || buffer[0] != '*') return args;

    size_t crlf = buffer.find("\r\n", pos);
    if (crlf == string::npos) return args;

    int num_args = stoi(buffer.substr(pos + 1, crlf - pos - 1));
    pos = crlf + 2;

    for (int i = 0; i < num_args; i++) {
        if (pos >= buffer.length() || buffer[pos] != '$') return {};

        crlf = buffer.find("\r\n", pos);
        if (crlf == string::npos) return {};

        int len = stoi(buffer.substr(pos + 1, crlf - pos - 1));
        pos = crlf + 2;

        if (pos + len + 2 > buffer.length()) return {};

        args.push_back(buffer.substr(pos, len));
        pos += len + 2;
    }
    buffer.erase(0, pos);
    return args;
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    set_nonblocking(server_fd);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    bind(server_fd, (sockaddr*)&address, sizeof(address));
    listen(server_fd, 10);

    int epoll_fd = epoll_create1(0);

    auto* server_state = new Client{server_fd, ""};
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.ptr = server_state;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);

    epoll_event events[MAX_EVENTS];

    cout << "Server pornit pe portul " << PORT << "...\n";

    while (true) {
        int num_ready = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);

        for (int i = 0; i < num_ready; i++) {
            auto* current = (Client*)events[i].data.ptr;

            if (current->fd == server_fd) {
                sockaddr_in client_addr{};
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &client_len);

                if (client_fd > 0) {
                    set_nonblocking(client_fd);
                    auto* new_client = new Client{client_fd, ""};

                    epoll_event c_event{};
                    c_event.events = EPOLLIN;
                    c_event.data.ptr = new_client;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &c_event);
                }
            } else {
                char temp_buf[1024];

                if (int bytes = read(current->fd, temp_buf, sizeof(temp_buf)); bytes <= 0) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current->fd, nullptr);
                    close(current->fd);
                    delete current;
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
        }
    }

    close(server_fd);
    return 0;
}