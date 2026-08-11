#ifndef REDIS_ROOM_H
#define REDIS_ROOM_H

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

    explicit Room(std::string name) : name(std::move(name)), last_access_time(0) {}
};

#endif //REDIS_ROOM_H
