#include "database.h"

Database::Database() {
    start_time = std::chrono::steady_clock::now();
    total_commands = 0;
}

void Database::set_client_room(int client_fd, const std::string& room_name) {
    client_rooms[client_fd] = room_name;
}

std::string Database::get_client_room(int client_fd) {
    // Daca clientul nu e in nicio camera, il punem in "default"
    if (client_rooms.find(client_fd) == client_rooms.end()) {
        client_rooms[client_fd] = "default";
    }
    return client_rooms[client_fd];
}

std::string Database::execute(int client_fd, const std::vector<std::string>& args) {
    if (args.empty()) return "";
    
    total_commands++;
    const std::string current_room = get_client_room(client_fd);
    std::string command = args[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "COMMAND" || command == "HELLO") {
        return "*0\r\n";
    }// pentru debugging ca habarn-am ce are paguba

    if (command == "ROOM" && args.size() >= 2) {
        set_client_room(client_fd, args[1]); // asociem file descriptorului room ul
        return "+OK\r\n";
    }

    if (command == "SET" && args.size() >= 3) {
        rooms[current_room][args[1]] = args[2];
        return "+OK\r\n";
    }

    if (command == "GET" && args.size() >= 2) {
        if (rooms[current_room].find(args[1]) != rooms[current_room].end()) {
            std::string val = rooms[current_room][args[1]];
            return "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
        }
        return "$-1\r\n"; // (nil)
    }

    if (command == "INFO") {
        auto now = std::chrono::steady_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
        size_t total_keys = 0;
        for (const auto& room : rooms) {
            total_keys += room.second.size();
        }

        std::string info_text = 
            "Uptime: " + std::to_string(uptime) + "s\n" +
            "Camere active: " + std::to_string(rooms.size()) + "\n" +
            "Chei totale: " + std::to_string(total_keys) + "\n" +
            "Comenzi procesate: " + std::to_string(total_commands) + "\n";
        // il ducem inapoi in format RESP
        return "$" + std::to_string(info_text.length()) + "\r\n" + info_text + "\r\n";
    }

    return "-ERR unknown command\r\n";
}