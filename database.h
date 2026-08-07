#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <unordered_map>
#include <map>
#include <chrono>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <fstream>
#include "pubsub.h"
#include "pool_allocator.h"

class CountMinSketch {
private:
    static constexpr int WIDTH = 4096; // trebuie sa fie putere a lui 2
    static constexpr int DEPTH = 4;
    uint16_t table[DEPTH][WIDTH] = {0};

    inline uint32_t fast_hash(const std::string& key, const uint32_t seed) {
        uint32_t h = seed;
        for (const char c : key) {
            h ^= static_cast<uint32_t>(c);
            h *= 0x5bd1e995;
            h ^= h >> 15;
        }
        return h;
    }

public:
    void record_access(const std::string& key) {
        for (int i = 0; i < DEPTH; ++i) {
            uint32_t h = fast_hash(key, (i + 1) * 0x9e3779b9) & (WIDTH - 1);
            if (table[i][h] < 0xFFFF) {
                table[i][h]++;
            }
        }
    }

    uint16_t estimate_frequency(const std::string& key) {
        uint16_t min_freq = 0xFFFF;
        for (int i = 0; i < DEPTH; ++i) {
            uint32_t h = fast_hash(key, (i + 1) * 0x9e3779b9) & (WIDTH - 1);
            if (table[i][h] < min_freq) {
                min_freq = table[i][h];
            }
        }
        return min_freq;
    }
};

struct Record {
    std::string value;
    long long expire_at;
    Record() : expire_at(0) {}
    Record(std::string v, const long long e) : value(std::move(v)), expire_at(e) {}
};
class Database {
private:
    std::unordered_map<std::string, std::unordered_map<std::string, Record*>> rooms;
    std::unordered_map<int, std::string> client_rooms;

    std::chrono::steady_clock::time_point start_time;
    long long total_commands;

    std::mutex db_mutex;

    std::unordered_map<std::string, long long> room_access_time;
    const size_t MAX_ACTIVE_ROOMS = 3;

    PubSubManager pubsub;

    PoolAllocator<Record, 1024> record_pool;

    void wakeup_room(const std::string& room_name);

    std::ofstream aof_file;
    bool is_recovering;
    CountMinSketch cms;
    const size_t MAX_KEYS_PER_ROOM = 10;

    void aof_append(const std::vector<std::string>& args);
    void aof_recover();

    void evict_despised_keys(const std::string& room_name);
public:
    Database();
    ~Database();

    void set_client_room(int client_fd, const std::string& room_name);
    std::string get_client_room(int client_fd);

    std::string execute(int client_fd, const std::vector<std::string>& args);

    void hibernate_inactive_rooms();
    void clean_expired_keys();
    void cleanup_client(int client_fd);
};

#endif