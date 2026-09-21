#include "aof_manager.h"
#include <iostream>
#include <iterator>
#include <system_error>
#include <filesystem>

AOFManager::AOFManager() : is_recovering(true) {
    aof_file.open("appendonly.aof", std::ios::app | std::ios::binary);
}

AOFManager::~AOFManager() {
    if (aof_file.is_open()) {
        aof_file.close();
    }
}

std::string AOFManager::encode_resp(const std::vector<std::string>& args) {
    std::string resp = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args) {
        resp += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    return resp;
}

void AOFManager::append(const std::string& room_name, const std::vector<std::string>& args) {
    std::lock_guard lock(aof_mutex);
    if (is_recovering || !aof_file.is_open()) return;

    std::vector<std::string> record;
    record.reserve(args.size() + 1);
    record.push_back(room_name);
    record.insert(record.end(), args.begin(), args.end());

    const std::string resp_cmd = encode_resp(record);
    aof_file.write(resp_cmd.c_str(), static_cast<std::streamsize>(resp_cmd.size()));
    aof_file.flush();
}

void AOFManager::recover(const std::function<void(const std::string&, const std::vector<std::string>&)>& execute_callback) {
    std::ifstream file("appendonly.aof", std::ios::binary);
    if (!file.is_open()) {
        is_recovering = false;
        return;
    }

    std::string buffer((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();

    size_t pos = 0;
    size_t recovered = 0;
    bool tail_corupt = false;

    // o comanda e "legacy" (fara prefix de camera) daca primul ei argument e chiar un command
    const auto is_legacy = [](const std::vector<std::string>& args) {
        if (args.empty()) return true;
        const std::string& c = args[0];
        return c == "SET" || c == "DEL" || c == "CRDTMERGE" || c == "ROOM" ||
               c == "GET" || c == "INFO" || c == "SAVE";
    };

    while (pos < buffer.length()) {
        if (buffer[pos] != '*') { tail_corupt = true; break; }

        const size_t header_end = buffer.find("\r\n", pos);
        if (header_end == std::string::npos) { tail_corupt = true; break; }

        int num_args = 0;
        try {
            num_args = std::stoi(buffer.substr(pos + 1, header_end - pos - 1));
        } catch (...) {
            tail_corupt = true;
            break;
        }
        if (num_args <= 0) { tail_corupt = true; break; }

        pos = header_end + 2;
        std::vector<std::string> args;
        bool ok = true;

        for (int i = 0; i < num_args && ok; ++i) {
            if (pos >= buffer.length() || buffer[pos] != '$') { ok = false; break; }

            const size_t len_end = buffer.find("\r\n", pos);
            if (len_end == std::string::npos) { ok = false; break; }

            int len = 0;
            try {
                len = std::stoi(buffer.substr(pos + 1, len_end - pos - 1));
            } catch (...) {
                ok = false;
                break;
            }
            if (len < 0) { ok = false; break; }

            pos = len_end + 2;
            if (pos + static_cast<size_t>(len) + 2 > buffer.length()) { ok = false; break; }

            args.emplace_back(buffer, pos, static_cast<size_t>(len));
            pos += static_cast<size_t>(len) + 2;
        }

        if (!ok) { tail_corupt = true; break; }

        if (is_legacy(args)) {
            // inregistrarile vechi nu au context de camera
            const std::vector<std::string> cmd(args.begin() + 1, args.end());
            execute_callback("default", cmd);
        } else {
            const std::vector<std::string> cmd(args.begin() + 1, args.end());
            execute_callback(args[0], cmd);
        }
        recovered++;
    }

    // coada scrisa partial (crash) se trunchiaza ca sa nu strice appends viitoare
    if (tail_corupt) {
        std::error_code ec;
        std::filesystem::resize_file("appendonly.aof", pos, ec);
        if (ec) {
            std::cerr << "AOF: nu am putut trunchia coada corupta: " << ec.message() << "\n";
        }
    }

    printf("AOF Recovery finalizat: %zu comenzi redate. Baza de date este pregatita!\n", recovered);
    is_recovering = false;
}
