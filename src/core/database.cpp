#include "database.h"
#include "../../metrics.h"
#include <ranges>
#include <algorithm>
#include <mutex>

static long long get_current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

Database::Database() {
    start_time = std::chrono::steady_clock::now();

    aof.recover([this](const std::vector<std::string>& args) {
        this->execute(-1, args);
    });
}

Database::~Database() {
}

void Database::wakeup_room(Room& room) {
    SnapshotManager::wakeup_room(room, record_pool);
}

void Database::set_client_room(const int client_fd, const std::string& room_name) {
    std::lock_guard lock(client_mutex);
    client_rooms[client_fd] = room_name;
}

std::string Database::get_client_room(const int client_fd) {
    std::lock_guard lock(client_mutex);
    if (!client_rooms.contains(client_fd)) {
        client_rooms[client_fd] = "default";
    }
    return client_rooms[client_fd];
}

std::string Database::execute(const int client_fd, const std::vector<std::string>& args) {
    if (args.empty()) return "";

    const std::string current_room = get_client_room(client_fd);
    std::string command = args[0];
    std::ranges::transform(command, command.begin(), ::toupper);

    if (command == "SUBSCRIBE") {
        if (args.size() >= 2) return pubsub.subscribe(client_fd, args[1]);
        return "-ERR Wrong number of arguments for SUBSCRIBE\r\n";
    }

    if (command == "PUBLISH") {
        if (args.size() >= 3) return pubsub.publish(args[1], args[2]);
        return "-ERR Wrong number of arguments for PUBLISH\r\n";
    }

    if (pubsub.is_subscribed(client_fd)) {
        return "-ERR Clientul este in mod SUBSCRIBE. Nu poti trimite comenzi de Database!\r\n";
    }

    total_commands.fetch_add(1, std::memory_order_relaxed);

    std::string target_room = current_room;
    if (command == "ROOM" && args.size() >= 2) {
        target_room = args[1];
    }

    Room* room_ptr = nullptr;
    {
        std::shared_lock shared_rooms_lock(rooms_mutex);
        if (rooms.contains(target_room)) {
            room_ptr = rooms[target_room].get();
        }
    }

    if (!room_ptr) {
        std::unique_lock exclusive_rooms_lock(rooms_mutex);
        if (!rooms.contains(target_room)) {
            if (rooms.size() >= MAX_ACTIVE_ROOMS) {
                return "-ERR RAM FULL. Te rog asteapta ca o alta camera sa hiberneze!\r\n";
            }
            rooms[target_room] = std::make_unique<Room>(target_room);
            room_ptr = rooms[target_room].get();
            wakeup_room(*room_ptr);
        } else {
            room_ptr = rooms[target_room].get();
        }
    }

    if (command == "COMMAND" || command == "HELLO") {
        return "*0\r\n";
    }

    if (command == "ROOM" && args.size() >= 2) {
        set_client_room(client_fd, args[1]);
        return "+OK\r\n";
    }

    std::lock_guard room_lock(room_ptr->room_mutex);
    room_ptr->last_access_time = get_current_time_ms();

    if (command == "GET") {
        if (args.size() == 2) {
            const std::string& key = args[1];
            global_metrics.total_gets.fetch_add(1, std::memory_order_relaxed);
            
            if (room_ptr->keys.contains(key)) {
                eviction.record_access(key);
                global_metrics.cache_hits.fetch_add(1, std::memory_order_relaxed);
                Record* r = room_ptr->keys[key];

                if (r->expire_at > 0 && get_current_time_ms() > r->expire_at) {
                    record_pool.deallocate(r);
                    room_ptr->keys.erase(key);
                    return "$-1\r\n";
                }
                std::string resp;
                resp.reserve(r->value.length() + 32);
                resp += "$";
                resp += std::to_string(r->value.length());
                resp += "\r\n";
                resp += r->value;
                resp += "\r\n";
                return resp;
            }

            global_metrics.cache_misses.fetch_add(1, std::memory_order_relaxed);

            if (!eviction.possibly_on_disk(key)) {
                global_metrics.bloom_prevented_disk_reads.fetch_add(1, std::memory_order_relaxed);
                return "$-1\r\n";
            }

            std::string cold_val = eviction.read_from_cold_storage(target_room, key);

            if (!cold_val.empty()) {
                room_ptr->keys[key] = record_pool.allocate(cold_val, 0);
                eviction.record_access(key);

                global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
                
                std::string resp;
                resp.reserve(cold_val.length() + 32);
                resp += "$";
                resp += std::to_string(cold_val.length());
                resp += "\r\n";
                resp += cold_val;
                resp += "\r\n";
                return resp;
            }
            return "$-1\r\n";
        }
        return "-ERR Wrong number of arguments for GET\r\n";
    }

    if (command == "SET") {
        if (args.size() >= 3) {
            const std::string& key = args[1];
            const std::string& value = args[2];
            global_metrics.total_sets.fetch_add(1, std::memory_order_relaxed);

            if (!room_ptr->keys.contains(key)) {
                global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
            }

            long long expire_at = 0; 

            if (room_ptr->keys.contains(key)) {
                room_ptr->keys[key]->value = value;
                room_ptr->keys[key]->expire_at = expire_at;
            } else {
                room_ptr->keys[key] = record_pool.allocate(value, expire_at);
            }

            eviction.record_access(key);
            
            aof.append(args);

            eviction.evict_despised_keys(*room_ptr, record_pool);

            return "+OK\r\n";
        }
        return "-ERR Wrong number of arguments for SET\r\n";
    }

    if (command == "INFO") {
        const auto now = std::chrono::steady_clock::now();
        const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
        size_t total_keys = 0;
        
        std::shared_lock shared_rooms_lock(rooms_mutex);
        size_t rooms_count = rooms.size();
        for (const auto& [name, ptr] : rooms) {
            if (name != target_room) { 
                // lock non-active rooms safely
                std::lock_guard r_lock(ptr->room_mutex);
                total_keys += ptr->keys.size();
            } else {
                total_keys += ptr->keys.size(); // target_room e deja lockat de if-ul in care suntem
            }
        }

        const std::string info_text =
            "Uptime: " + std::to_string(uptime) + "s\n" +
            "Camere active: " + std::to_string(rooms_count) + "\n" +
            "Chei totale: " + std::to_string(total_keys) + "\n" +
            "Comenzi procesate: " + std::to_string(total_commands.load(std::memory_order_relaxed)) + "\n";
        
        std::string resp;
        resp.reserve(info_text.length() + 32);
        resp += "$";
        resp += std::to_string(info_text.length());
        resp += "\r\n";
        resp += info_text;
        resp += "\r\n";
        return resp;
    }

    if (command == "SAVE") {
        return "+OK AOF is active and up to date\r\n";
    }

    return "-ERR unknown command\r\n";
}

void Database::cleanup_client(const int client_fd) {
    pubsub.remove_client(client_fd);

    std::lock_guard lock(client_mutex);
    client_rooms.erase(client_fd);
}

void Database::hibernate_inactive_rooms() {
    std::unique_lock exclusive_rooms_lock(rooms_mutex);
    
    long long now = get_current_time_ms();
    std::vector<std::string> rooms_to_sleep;

    for (const auto& [room_name, ptr] : rooms) {
        if (now - ptr->last_access_time > 10000 && room_name != "default") {
            rooms_to_sleep.push_back(room_name);
        }
    }

    for (const std::string& r : rooms_to_sleep) {
        if (rooms.contains(r)) {
            std::lock_guard room_lock(rooms[r]->room_mutex);
            SnapshotManager::hibernate_room(*rooms[r], record_pool);
            rooms.erase(r);
        }
    }
}

void Database::clean_expired_keys() {
    std::shared_lock shared_rooms_lock(rooms_mutex);
    const long long now = get_current_time_ms();

    for (const auto& [name, ptr] : rooms) {
        std::lock_guard room_lock(ptr->room_mutex);
        for (auto it = ptr->keys.begin(); it != ptr->keys.end(); ) {
            if (it->second->expire_at > 0 && it->second->expire_at < now) {
                record_pool.deallocate(it->second);
                it = ptr->keys.erase(it);
            } else {
                ++it;
            }
        }
    }
}
