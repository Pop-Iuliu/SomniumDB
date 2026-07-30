#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <unordered_map>
#include <map>
#include <chrono>
#include <vector>
#include <algorithm>

class Database {
private:
    std::unordered_map<std::string, std::map<std::string, std::string>> rooms;
    std::unordered_map<int, std::string> client_rooms;

    std::chrono::steady_clock::time_point start_time;
    long long total_commands;

public:
    Database();
    void set_client_room(int client_fd, const std::string& room_name);
    std::string get_client_room(int client_fd);

    std::string execute(int client_fd, const std::vector<std::string>& args);
};

#endif