#include "aof_manager.h"
#include <iostream>

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

void AOFManager::append(const std::vector<std::string>& args) {
    std::lock_guard lock(aof_mutex);
    if (is_recovering || !aof_file.is_open()) return;

    const std::string resp_cmd = encode_resp(args);
    aof_file.write(resp_cmd.c_str(), resp_cmd.size());
    // aof_file.flush();
}

void AOFManager::recover(const std::function<void(const std::vector<std::string>&)>& execute_callback) {
    std::ifstream file("appendonly.aof", std::ios::binary);
    if (!file.is_open()) {
        is_recovering = false;
        return;
    }

    std::string buffer;
    char temp[4096];

    while (file.read(temp, sizeof(temp))) {
        buffer.append(temp, file.gcount());
    }
    buffer.append(temp, file.gcount());
    file.close();

    size_t pos = 0;
    while (pos < buffer.length()) {
        if (buffer[pos] != '*') break;

        size_t crlf = buffer.find("\r\n", pos);
        if (crlf == std::string::npos) break;

        int num_args = std::stoi(buffer.substr(pos + 1, crlf - pos - 1));
        pos = crlf + 2;

        std::vector<std::string> args;
        for (int i = 0; i < num_args; i++) {
            crlf = buffer.find("\r\n", pos);
            int len = std::stoi(buffer.substr(pos + 1, crlf - pos - 1));
            pos = crlf + 2;
            args.push_back(buffer.substr(pos, len));
            pos += len + 2;
        }

        execute_callback(args);
    }
    printf("AOF Recovery finalizat. Baza de date este pregatita!\n");
    is_recovering = false;
}
