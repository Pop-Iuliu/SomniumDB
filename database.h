#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <unordered_map>
#include <map>
#include <chrono>
#include <vector>
#include <algorithm>
#include <thread>

struct Record {
    std::string value;
    long long expire_at;
};

class Database {
private:
    std::unordered_map<std::string, std::map<std::string, Record>> rooms;
    std::unordered_map<int, std::string> client_rooms;

    std::chrono::steady_clock::time_point start_time;
    long long total_commands;

    std::mutex db_mutex;
public:
    Database();
    void set_client_room(int client_fd, const std::string& room_name);
    std::string get_client_room(int client_fd);

    std::string execute(int client_fd, const std::vector<std::string>& args);
    bool save_to_disk();
    void load_from_disk();

    void clean_expired_keys();
};

#endif