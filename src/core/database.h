#pragma once

#include <string>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <atomic>
#include "../../pubsub.h"
#include "../../pool_allocator.h"
#include "../storage/aof_manager.h"
#include "../storage/eviction_manager.h"
#include "../storage/snapshot_manager.h"
#include "../../record.h"
#include "room.h"

class Database {
private:
    // shared_ptr ca o comanda in curs sa poata supravietuii hibernarii
    std::unordered_map<std::string, std::shared_ptr<Room>> rooms;
    std::unordered_map<int, std::string> client_rooms;

    std::chrono::steady_clock::time_point start_time;
    std::atomic<long long> total_commands{0};

    std::shared_mutex rooms_mutex; // protejeaza rooms hashmap
    std::mutex client_mutex;       // protejeaza client_rooms si accesul global (non-room)

    static constexpr size_t MAX_ACTIVE_ROOMS = 3;
    static constexpr long long ROOM_IDLE_MS = 10000;
    static constexpr int LOCAL_NODE_ID = 1;

    PubSubManager pubsub;
    PoolAllocator<Record, 1024> record_pool;

    AOFManager aof;
    EvictionManager eviction;

    // intoarce nullptr daca s-a atins limita de camere active
    std::shared_ptr<Room> get_or_create_room(const std::string& name);

    // dispecerarea lock-uita pe camera; folosita si de replay-ul AOF
    std::string execute_in_room(const std::string& room_name, const std::vector<std::string>& args);

    std::string handle_get(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_set(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_del(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_crdtmerge(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_info(Room& room);

    static std::string bulk_string(const std::string& payload);

public:
    Database();
    ~Database();

    void set_client_room(int client_fd, const std::string& room_name);
    std::string get_client_room(int client_fd);

    // livrarea mesajelor Pub/Sub prin coada de output a serverului
    void set_message_sink(std::function<void(int, const std::string&)> fn) {
        pubsub.set_message_sink(std::move(fn));
    }

    std::string execute(int client_fd, const std::vector<std::string>& args);

    void hibernate_inactive_rooms();
    void clean_expired_keys();
    void cleanup_client(int client_fd);
};
