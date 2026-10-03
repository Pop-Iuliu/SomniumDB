#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <atomic>
#include <functional>
#include "../../pubsub.h"
#include "../../pool_allocator.h"
#include "../storage/aof_manager.h"
#include "../storage/eviction_manager.h"
#include "../storage/snapshot_manager.h"
#include "../../record.h"
#include "room.h"

class Database {
private:
    // shared_ptr ca o comanda in curs sa poata supravietuii hibernarii
    std::unordered_map<std::string, std::shared_ptr<Room>> rooms;
    std::unordered_map<int, std::string> client_rooms;

    std::chrono::steady_clock::time_point start_time;
    std::atomic<long long> total_commands{0};

    std::shared_mutex rooms_mutex; // protejeaza rooms hashmap (contractul complet: room.h)
    std::mutex client_mutex;       // protejeaza client_rooms si accesul global (non-room)
    std::atomic<size_t> active_rooms{0}; // camere care nu sunt Sleeping

    static constexpr size_t MAX_ACTIVE_ROOMS = 3;
    static constexpr const char* ROOMS_FULL =
        "-ERR ROOMS FULL: active room budget reached, ROOM.HIBERNATE one or wait\r\n";
    static constexpr long long ROOM_IDLE_MS = 10000;
    uint32_t local_node_id = 1; // SOMNIUM_NODE_ID: versiunea CRDT a scrierilor locale

    PubSubManager pubsub;
    PoolAllocator<Record, 1024> record_pool;

    AOFManager aof;
    EvictionManager eviction;

    // nullptr doar daca camera nu exista si create == false
    std::shared_ptr<Room> find_room(const std::string& name, bool create);

    // ambele presupun room_mutex prins. activate: false = buget plin (bypass
    // la recovery: redarea nu pierde camere din cauza bugetului).
    // hibernate: false = snapshot esuat, camera ramane activa si citibila.
    bool activate(Room& room, bool over_budget = false);
    bool hibernate(Room& room);
    // creeaza la nevoie si activeaza, luand singur lock-ul camerei
    bool wake_room(const std::string& name);

    // dispecerarea lock-uita pe camera; folosita si de replay-ul AOF
    std::string execute_in_room(const std::string& room_name, const std::vector<std::string>& args);

    // replay verbatim al unei mutatii persistate: aplica starea rezultata
    // (valoare, expirare, versiune CRDT) asa cum a fost serializata, fara sa
    // regenereze timestampuri sau sa re-evalueze comanda
    void replay_record(const AofRecord& rec);

    // AOF rescris ca stare curenta; folosit si pentru migrarea v1/v2 -> v3
    bool rewrite_aof();

    // cheia logica: RAM, apoi cold storage (reincarcata in RAM); cheile expirate
    // conteaza ca absente. nullptr = cheia nu exista.
    Record* lookup(const std::string& room_name, Room& room, const std::string& key);
    // singurele locuri care schimba continutul RAM al unei camere, ca numarul
    // de chei si octetii rezidenti sa nu poata diverge (presupun room_mutex prins)
    Record* insert(Room& room, const std::string& key); // goala; apelantul o completeaza
    void set_value(Room& room, Record* r, const std::string& value);
    void erase(Room& room, std::unordered_map<std::string, Record*>::iterator it);

    std::string handle_get(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_set(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_mset(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_mget(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_exists(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_expire(const std::string& room_name, Room& room, const std::vector<std::string>& args,
                              long long scale);
    std::string handle_ttl(const std::string& room_name, Room& room, const std::vector<std::string>& args, bool millis);
    std::string handle_del(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_crdtmerge(const std::string& room_name, Room& room, const std::vector<std::string>& args);
    std::string handle_info();
    std::string handle_room_admin(const std::string& command, const std::vector<std::string>& args);

    static std::string bulk_string(const std::string& payload);

    // protocolul clientului a carui comanda ruleaza acum; il citesc doar
    // raspunsurile care difera intre RESP2 si RESP3 (thread-ul de comenzi)
    bool resp3_ = false;
    std::string nil() const { return resp3_ ? "_\r\n" : "$-1\r\n"; }
    std::string map_header(const size_t pairs) const {
        return resp3_ ? "%" + std::to_string(pairs) + "\r\n" : "*" + std::to_string(2 * pairs) + "\r\n";
    }

public:
    Database();
    ~Database();

    void set_client_room(int client_fd, const std::string& room_name);
    std::string get_client_room(int client_fd);

    // livrarea mesajelor Pub/Sub prin coada de output a serverului
    void set_message_sink(std::function<void(int, const std::string&)> fn) {
        pubsub.set_message_sink(std::move(fn));
    }

    // resp3: protocolul negociat de conexiune prin HELLO (tinut de server)
    std::string execute(int client_fd, const std::vector<std::string>& args, bool resp3 = false);

    // sincronizare AOF granulata (politica everysec); apelata de watchdog
    void sync_aof_if_due() { aof.sync_if_due(); }

    void hibernate_inactive_rooms();
    void clean_expired_keys();
    void cleanup_client(int client_fd);
};
