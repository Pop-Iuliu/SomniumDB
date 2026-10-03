#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <mutex>
#include "../../record.h"
#include "../../metrics.h"

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

// costul estimat al unei inregistrari in RAM: cheie, valoare si o regie fixa
// (Record, nodul din hashmap, headerul cheii). O estimare, nu contabilitate de alocator.
constexpr size_t kRecordOverhead = sizeof(Record) + sizeof(std::string) + 2 * sizeof(void*);
inline size_t record_cost(const std::string& key, const Record& r) {
    return key.size() + r.value.size() + kRecordOverhead;
}

class Room {
public:
    std::string name;
    std::unordered_map<std::string, Record*> keys;
    std::mutex room_mutex;
    std::atomic<long long> last_access_time{0};
    std::atomic<RoomState> state{RoomState::Sleeping};
    size_t bytes = 0; // octetii rezidenti estimati, sub room_mutex

    explicit Room(std::string name) : name(std::move(name)) {}

    // singurul loc prin care se schimba octetii: camera si totalul global
    void charge(const long long delta) {
        bytes += delta;
        global_metrics.resident_bytes.fetch_add(delta, std::memory_order_relaxed);
    }

    size_t keys_cost() const {
        size_t total = 0;
        for (const auto& [key, r] : keys) total += record_cost(key, *r);
        return total;
    }
};
