#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <mutex>
#include "../../record.h"

// Ciclul de viata: Sleeping -> Loading -> Active -> Hibernating -> Sleeping.
//
// Contract de lock-uri si ownership:
//   - `keys` si orice tranzitie de stare se ating doar sub room_mutex; sub
//     lock starea e mereu Sleeping sau Active (Loading/Hibernating sunt
//     vizibile doar cititorilor fara lock, ex. ROOM.INFO in timpul I/O-ului)
//   - `state` si `last_access_time` sunt atomice: watchdog-ul si inspectia
//     le citesc fara sa astepte dupa I/O-ul de snapshot al camerei
//   - ordinea: rooms_mutex inaintea room_mutex, niciodata invers
//   - orice stare in afara de Sleeping ocupa un loc din bugetul de camere active
//   - camerele nu se sterg din registru: un shared_ptr obtinut inainte de
//     hibernare indica mereu camera inregistrata
enum class RoomState { Sleeping, Loading, Active, Hibernating };

class Room {
public:
    std::string name;
    std::unordered_map<std::string, Record*> keys;
    std::mutex room_mutex;
    std::atomic<long long> last_access_time{0};
    std::atomic<RoomState> state{RoomState::Sleeping};

    explicit Room(std::string name) : name(std::move(name)) {}
};
