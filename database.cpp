#include "database.h"
#include "json.hpp"
#include <fstream>

/*
    functii utilitare
*/

static long long get_current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

static void to_json(nlohmann::json& j, const Record& r) {
    j = nlohmann::json{
        {"value", r.value},
        {"expire_at", r.expire_at}
    };
}

static void from_json(const nlohmann::json& j, Record& r) {
    j.at("value").get_to(r.value);
    j.at("expire_at").get_to(r.expire_at);
}

/*
    main stuff
*/

Database::Database() {
    start_time = std::chrono::steady_clock::now();
    load_from_disk();
    total_commands = 0;
}

void Database::set_client_room(const int client_fd, const std::string& room_name) {
    client_rooms[client_fd] = room_name;
}

std::string Database::get_client_room(const int client_fd) {
    // Daca clientul nu e in nicio camera, il punem in "default"
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
        if (args.size() >= 2) {
            return pubsub.subscribe(client_fd, args[1]);
        }
        return "-ERR Wrong number of arguments for SUBSCRIBE\r\n";
    }

    if (command == "PUBLISH") {
        if (args.size() >= 3) {
            return pubsub.publish(args[1], args[2]);
        }
        return "-ERR Wrong number of arguments for PUBLISH\r\n";
    }

    if (pubsub.is_subscribed(client_fd)) {
        return "-ERR Clientul este in mod SUBSCRIBE. Nu poti trimite comenzi de Database!\r\n";
    }

    std::lock_guard lock(db_mutex);

    total_commands++;

    std::string target_room = current_room;
    if (command == "ROOM" && args.size() >= 2) {
        target_room = args[1];
    }

    if (!rooms.contains(target_room)) {
        if (rooms.size() >= MAX_ACTIVE_ROOMS) {
            return "-ERR RAM FULL. Te rog asteapta ca o alta camera sa hiberneze!\r\n";
        }
        wakeup_room(target_room);
    }

    room_access_time[target_room] = get_current_time_ms();

    if (command == "COMMAND" || command == "HELLO") {
        return "*0\r\n";
    }

    if (command == "ROOM" && args.size() >= 2) {
        set_client_room(client_fd, args[1]);
        return "+OK\r\n";
    }

    if (command == "SET") {
        if (args.size() >= 3) {
            const std::string& key = args[1];
            const std::string& value = args[2];
            long long expire_at = 0;

            if (args.size() >= 5 && args[3] == "EX") {
                try {
                    const long long ttl_seconds = std::stoll(args[4]);
                    expire_at = get_current_time_ms() + (ttl_seconds * 1000);
                } catch (...) {
                    return "-ERR Invalid TTL format\r\n";
                }
            }

            if (rooms[current_room].contains(key)) {
                rooms[current_room][key]->value = value;
                rooms[current_room][key]->expire_at = expire_at;
            } else {
                rooms[current_room][key] = record_pool.allocate(value, expire_at);
            }
            return "+OK\r\n";
        }
        return "-ERR Wrong number of arguments for SET\r\n";
    }

    if (command == "GET") {
        if (args.size() == 2) {
            const std::string& key = args[1];

            if (rooms.contains(current_room) && rooms[current_room].contains(key)) {
                Record* r = rooms[current_room][key];

                if (r->expire_at > 0 && r->expire_at < get_current_time_ms()) {
                    record_pool.deallocate(r);
                    rooms[current_room].erase(key);
                    return "$-1\r\n";
                }
                const std::string val = r->value;
                return "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
            }
            return "$-1\r\n";
        }
        return "-ERR Wrong number of arguments for GET\r\n";
    }

    if (command == "INFO") {
        const auto now = std::chrono::steady_clock::now();
        const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
        size_t total_keys = 0;
        for (const auto &val: rooms | std::views::values) { // asta mi-a recomandat-o clang tidy, idk ce face si cum (TO LOOK UP AND LEARN !!!!!!!!!!!!)
            total_keys += val.size();
        }

        const std::string info_text =
            "Uptime: " + std::to_string(uptime) + "s\n" +
            "Camere active: " + std::to_string(rooms.size()) + "\n" +
            "Chei totale: " + std::to_string(total_keys) + "\n" +
            "Comenzi procesate: " + std::to_string(total_commands) + "\n";
        // il ducem inapoi in format RESP
        return "$" + std::to_string(info_text.length()) + "\r\n" + info_text + "\r\n";
    }

    if (command == "SAVE") {
        if (save_to_disk()) {
            return "+OK\r\n"; // Raspundem clientului ca totul a mers perfect
        } else {
            return "-ERR Failed to save to disk\r\n";
        }
    }

    return "-ERR unknown command\r\n";
}

void Database::cleanup_client(const int client_fd) {
    pubsub.remove_client(client_fd);

    std::lock_guard lock(db_mutex);
    client_rooms.erase(client_fd);
}

/*
    functii pentru Snapshotting
*/

bool Database::save_to_disk() {
    try {
        nlohmann::json j;
        // Dereferențiem manual pointerul (*record_ptr) pentru a apela to_json-ul tău
        for (const auto& [room_name, keys] : rooms) {
            for (const auto& [key, record_ptr] : keys) {
                j[room_name][key] = *record_ptr;
            }
        }
        std::ofstream file("dump.json");
        file << j.dump(4);
        return true;
    } catch (...) {
        return false;
    }
}

void Database::load_from_disk() {
    std::ifstream file("dump.json");
    if (file.is_open()) {
        try {
            nlohmann::json j;
            file >> j;
            for (const auto& [room_name, keys] : j.items()) {
                for (const auto& [key, val] : keys.items()) {
                    Record* r = record_pool.allocate();
                    val.get_to(*r);
                    rooms[room_name][key] = r;
                }
            }
        } catch (...) {}
    }
}

void Database::wakeup_room(const std::string& room_name) {
    std::string filename = "room_" + room_name + ".json";

    if (std::ifstream file(filename); file.is_open()) {
        try {
            nlohmann::json j;
            file >> j;

            rooms[room_name].clear();

            for (auto& [key, val] : j.items()) {
                Record* r = record_pool.allocate();
                r->value = val.at("value").get<std::string>();
                r->expire_at = val.at("expire_at").get<long long>();
                rooms[room_name][key] = r;
            }
        } catch (const std::exception& e) {
            printf("-ERR eroare citire %s: %s\n", filename.c_str(), e.what());
            rooms[room_name] = {};
        }

        file.close();
        std::remove(filename.c_str());
    } else {
        rooms[room_name] = {};
    }
}

void Database::hibernate_inactive_rooms() {
    std::lock_guard lock(db_mutex);
    long long now = get_current_time_ms();
    std::vector<std::string> rooms_to_sleep;

    for (const auto& [room_name, last_time] : room_access_time) {
        if (now - last_time > 10000 && room_name != "default") {
            rooms_to_sleep.push_back(room_name);
        }
    }
    for (const std::string& r : rooms_to_sleep) {
        std::string filename = "room_" + r + ".json";
        std::ofstream file(filename);

        nlohmann::json j;
        for (const auto& [key, record_ptr] : rooms[r]) {
            j[key] = *record_ptr;
            record_pool.deallocate(record_ptr);
        }

        file << j.dump(4);
        file.close();
        rooms.erase(r);
        room_access_time.erase(r);
    }
}

// TTL Watchdog

void Database::clean_expired_keys() {
    std::lock_guard lock(db_mutex);

    const long long now = get_current_time_ms();
    int deleted_count = 0;

    for (auto &room_keys: rooms | std::views::values) {
        for (auto it = room_keys.begin(); it != room_keys.end(); ) {
            if (it->second->expire_at > 0 && it->second->expire_at < now) {
                record_pool.deallocate(it->second);
                it = room_keys.erase(it);
                deleted_count++;
            } else {
                ++it;
            }
        }
    }
    /*
    if (deleted_count > 0) {
        printf("🐶👀 (cainele cu gitul lunge) a sters %d chei expirate din RAM.\n", deleted_count);
    } */
}
