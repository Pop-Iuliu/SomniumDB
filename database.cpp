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

void Database::set_client_room(int client_fd, const std::string& room_name) {
    client_rooms[client_fd] = room_name;
}

std::string Database::get_client_room(int client_fd) {
    // Daca clientul nu e in nicio camera, il punem in "default"
    if (!client_rooms.contains(client_fd)) {
        client_rooms[client_fd] = "default";
    }
    return client_rooms[client_fd];
}

std::string Database::execute(int client_fd, const std::vector<std::string>& args) {
    if (args.empty()) return "";

    std::lock_guard lock(db_mutex);

    total_commands++;
    const std::string current_room = get_client_room(client_fd);
    std::string command = args[0];
    std::ranges::transform(command, command.begin(), ::toupper);

    if (command == "COMMAND" || command == "HELLO") {
        return "*0\r\n";
    }// pentru debugging ca habarn-am ce are paguba

    if (command == "ROOM" && args.size() >= 2) {
        set_client_room(client_fd, args[1]); // asociem file descriptorului room ul
        return "+OK\r\n";
    }

    if (command == "SET") {
        if (args.size() >= 3) {
            const std::string& key = args[1];
            const std::string& value = args[2];
            long long expire_at = 0; // default - no delete :)

            if (args.size() >= 5 && args[3] == "EX") {
                try {
                    const long long ttl_seconds = std::stoll(args[4]);
                    expire_at = get_current_time_ms() + (ttl_seconds * 1000);
                } catch (...) {
                    return "-ERR Invalid TTL format\r\n";
                }
            }

            rooms[current_room][key] = {value, expire_at};
            return "+OK\r\n";
        }
        return "-ERR Wrong number of arguments for SET\r\n";
    }

    if (command == "GET") {
        if (args.size() == 2) {
            const std::string& key = args[1];

            if (rooms.count(current_room) && rooms[current_room].count(key)) {
                auto&[value, expire_at] = rooms[current_room][key];

                if (expire_at > 0 && expire_at < get_current_time_ms()) {
                    rooms[current_room].erase(key); // O stergem din memorie
                    return "$-1\r\n"; // Returnam NULL
                }
                const std::string val = value;
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

/*
    functii pentru Snapshotting
*/

bool Database::save_to_disk() {
    try {
        const nlohmann::json j = rooms;
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
            j.get_to(rooms);
        } catch (...) {
        }
    }
}

// TTL Watchdog

void Database::clean_expired_keys() {
    std::lock_guard lock(db_mutex); // Punem lacatul si aici!

    const long long now = get_current_time_ms();
    int deleted_count = 0;

    for (auto &room_keys: rooms | std::views::values) {
        for (auto it = room_keys.begin(); it != room_keys.end(); ) {
            if (it->second.expire_at > 0 && it->second.expire_at < now) {
                it = room_keys.erase(it);
                deleted_count++;
            } else {
                ++it;
            }
        }
    }

    if (deleted_count > 0) {
        printf("🐶👀 (cainele cu gitul lunge) a sters %d chei expirate din RAM.\n", deleted_count);
    }
}
