#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include "../../record.h"

class Room {
public:
    std::string name;
    std::unordered_map<std::string, Record*> keys;
    std::mutex room_mutex;
    long long last_access_time;
    bool hibernated;

    explicit Room(std::string name) : name(std::move(name)), last_access_time(0), hibernated(false) {}
};
