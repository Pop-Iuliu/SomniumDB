#include "database.h"
#include "../../metrics.h"
#include "../storage/fs_util.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <climits>
#include <cstdlib>

namespace {
    long long get_current_time_ms() {
        // SOMNIUM_CLOCK_OFFSET_MS muta ceasul: ceas controlabil pentru testele
        // de expirare peste restart (timp "trecut" cat serverul era oprit)
        static const long long offset = [] {
            long long v = 0;
            const char* env = getenv("SOMNIUM_CLOCK_OFFSET_MS");
            return env && fsutil::parse_i64(env, &v) ? v : 0;
        }();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count() + offset;
    }
}

Database::Database() {
    start_time = std::chrono::steady_clock::now();

    // 1) redare istoric (snapshot-urile se incarca sub prima mutatie a fiecarei camere,
    //    apoi mutatiile AOF se aplica peste ele: un snapshot vechi nu poate
    //    suprascrie starea mai noua redata din AOF)
    aof.recover([this](const AofRecord& rec) {
        this->replay_record(rec);
    });

    // 2) migrare explicita: AOF-urile vechi (v1/v2) sau corupte ajung in v3
    if (aof.needs_rewrite()) {
        migrate_aof_to_v3();
    }

    // 3) abia acum deschidem append-ul; fisier nou primeste header v3
    if (!aof.open_file()) {
        fprintf(stderr, "AOF: serverul ruleaza FARA persistenta (deschiderea a esuat)!\n");
    }
}

Database::~Database() = default;

// migrarea nu schimba starea, doar o re-serialiaza in formatul v3 atomically:
// temporar -> fsync -> rename. Esuarea in orice punct lasa fisierul vechi neatins.
void Database::migrate_aof_to_v3() {
    if (!aof.start_rewrite()) return;

    bool ok = true;
    {
        std::shared_lock rooms_lock(rooms_mutex);
        for (auto& [name, room] : rooms) {
            if (room->state == RoomState::Sleeping) continue; // snapshot-ul ei este persistenta
            std::lock_guard room_lock(room->room_mutex);
            for (const auto& [key, rec] : room->keys) {
                AofRecord out;
                out.room = name;
                out.args = {"SET", key, rec->value};
                out.expire_at = rec->expire_at;
                out.timestamp_ms = rec->timestamp_ms;
                out.node_id = rec->node_id;
                if (!aof.append_rewrite(out)) {
                    ok = false;
                    break;
                }
            }
            if (!ok) break;
        }
    }

    if (!ok) {
        aof.abort_rewrite();
        printf("AOF: migrarea v3 a esuat; fisierul vechi ramane neatins (se reincearca la urmatorul restart)\n");
        return;
    }
    aof.commit_rewrite();
}

void Database::replay_record(const AofRecord& rec) {
    if (rec.args.empty()) return;

    std::string command = rec.args[0];
    std::ranges::transform(command, command.begin(), ::toupper);

    // camera adormita: snapshot-ul (mai vechi) se incarca acum, sub lock-ul ei,
    // si mutatiile din AOF se aplica peste el in ordinea istorica. Recovery-ul
    // trece peste buget: redarea nu are voie sa piarda mutatii.
    const std::shared_ptr<Room> room = find_room(rec.room, true);
    std::lock_guard room_lock(room->room_mutex);
    activate(*room, true);

    // starea rezultata, aplicata verbatim. Live, CRDTMERGE ajunge in AOF doar
    // cand a convers, deci nu se re-evalueaza nici el (live, o cheie expirata
    // conteaza ca absenta; o comparatie la replay ar putea da alt rezultat)
    const bool is_merge = command == "CRDTMERGE" && rec.args.size() >= 5;
    if (is_merge || (command == "SET" && rec.args.size() >= 3)) {
        uint64_t ts = rec.timestamp_ms;
        uint64_t node = rec.node_id;
        // formatele vechi nu au meta: versiunea merge-ului vine din argumente
        if (is_merge && (!fsutil::parse_u64(rec.args[3], &ts) || !fsutil::parse_u64(rec.args[4], &node) ||
                         node > UINT32_MAX)) {
            return;
        }
        Record*& r = room->keys[rec.args[1]];
        if (!r) {
            r = record_pool.construct();
            global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
        }
        r->value = rec.args[2];
        r->expire_at = rec.expire_at;
        r->timestamp_ms = ts;
        r->node_id = static_cast<uint32_t>(node);
        return;
    }

    if (command == "DEL" && rec.args.size() >= 2) {
        if (const auto it = room->keys.find(rec.args[1]); it != room->keys.end()) {
            record_pool.destroy(it->second);
            room->keys.erase(it);
            global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
        }
        return;
    }

    // comenzi de citire sau necunoscute (AOF-uri foarte vechi): ignorate la replay
}

std::string Database::bulk_string(const std::string& payload) {
    std::string resp;
    resp.reserve(payload.length() + 32);
    resp += '$';
    resp += std::to_string(payload.length());
    resp += "\r\n";
    resp += payload;
    resp += "\r\n";
    return resp;
}

void Database::set_client_room(const int client_fd, const std::string& room_name) {
    std::lock_guard lock(client_mutex);
    client_rooms[client_fd] = room_name;
}

std::string Database::get_client_room(const int client_fd) {
    std::lock_guard lock(client_mutex);
    if (!client_rooms.contains(client_fd)) {
        client_rooms[client_fd] = "default";
    }
    return client_rooms[client_fd];
}

std::shared_ptr<Room> Database::find_room(const std::string& name, const bool create) {
    {
        std::shared_lock lock(rooms_mutex);
        if (const auto it = rooms.find(name); it != rooms.end()) {
            return it->second;
        }
    }
    if (!create) return nullptr;

    std::unique_lock lock(rooms_mutex);
    auto& room = rooms[name];
    if (!room) room = std::make_shared<Room>(name);
    return room;
}

bool Database::activate(Room& room, const bool over_budget) {
    room.last_access_time = get_current_time_ms();
    if (room.state == RoomState::Active) return true;

    // rezervam locul inainte de I/O: bugetul nu poate fi depasit nici de
    // activari concurente, fara sa parcurgem registrul sub lock
    size_t active = active_rooms.load();
    do {
        if (active >= MAX_ACTIVE_ROOMS && !over_budget) return false;
    } while (!active_rooms.compare_exchange_weak(active, active + 1));

    room.state = RoomState::Loading;
    SnapshotManager::wakeup_room(room, record_pool);
    global_metrics.keys_in_ram.fetch_add(room.keys.size(), std::memory_order_relaxed);
    room.state = RoomState::Active;
    return true;
}

bool Database::hibernate(Room& room) {
    if (room.state == RoomState::Sleeping) return true;

    room.state = RoomState::Hibernating;
    const size_t resident = room.keys.size();
    if (!SnapshotManager::hibernate_room(room, record_pool)) {
        room.state = RoomState::Active;
        return false;
    }
    global_metrics.keys_in_ram.fetch_sub(resident, std::memory_order_relaxed);
    room.state = RoomState::Sleeping;
    active_rooms.fetch_sub(1);
    return true;
}

bool Database::wake_room(const std::string& name) {
    const std::shared_ptr<Room> room = find_room(name, true);
    std::lock_guard lock(room->room_mutex);
    return activate(*room);
}

std::string Database::execute(const int client_fd, const std::vector<std::string>& args) {
    if (args.empty()) return "";

    const std::string current_room = get_client_room(client_fd);
    std::string command = args[0];
    std::ranges::transform(command, command.begin(), ::toupper);

    if (command == "SUBSCRIBE") {
        if (args.size() >= 2) return pubsub.subscribe(client_fd, args[1]);
        return "-ERR Wrong number of arguments for SUBSCRIBE\r\n";
    }

    if (command == "PUBLISH") {
        if (args.size() >= 3) return pubsub.publish(args[1], args[2]);
        return "-ERR Wrong number of arguments for PUBLISH\r\n";
    }

    if (pubsub.is_subscribed(client_fd)) {
        return "-ERR Clientul este in mod SUBSCRIBE. Nu poti trimite comenzi de Database!\r\n";
    }

    total_commands.fetch_add(1, std::memory_order_relaxed);

    if (command == "COMMAND" || command == "HELLO") {
        return "*0\r\n";
    }

    // ROOM selecteaza (si trezeste); comenzile de administrare au prefixul
    // "ROOM." ca nume precum LIST sa ramana camere valide
    if (command == "ROOM") {
        if (args.size() != 2) return "-ERR Wrong number of arguments for ROOM\r\n";
        if (!wake_room(args[1])) return ROOMS_FULL;
        set_client_room(client_fd, args[1]);
        return "+OK\r\n";
    }

    if (command == "ROOMS" || command.starts_with("ROOM.")) {
        return handle_room_admin(command, args);
    }

    if (command == "COMPACT") {
        const long long reclaimed = eviction.compact(get_current_time_ms());
        return reclaimed < 0 ? "-ERR compaction failed, previous cold file kept\r\n"
                             : ":" + std::to_string(reclaimed) + "\r\n";
    }

    return execute_in_room(current_room, args);
}

std::string Database::handle_room_admin(const std::string& command, const std::vector<std::string>& args) {
    if (command == "ROOMS") {
        std::shared_lock lock(rooms_mutex);
        std::string out = "*" + std::to_string(rooms.size()) + "\r\n";
        for (const auto& [name, room] : rooms) out += bulk_string(name);
        return out;
    }

    if (args.size() != 2) return "-ERR Wrong number of arguments for " + command + "\r\n";
    const std::string& name = args[1];

    if (command == "ROOM.WAKE") return wake_room(name) ? "+OK\r\n" : ROOMS_FULL;

    const std::shared_ptr<Room> room = find_room(name, false);
    if (!room) return "-ERR no such room\r\n";

    if (command == "ROOM.HIBERNATE") {
        if (name == "default") return "-ERR the default room is pinned\r\n";
        std::lock_guard lock(room->room_mutex);
        return hibernate(*room) ? "+OK\r\n" : "-ERR snapshot failed, room stays active\r\n";
    }

    if (command == "ROOM.INFO") {
        // starea se citeste fara lock (nu asteptam dupa I/O-ul de snapshot);
        // cheile rezidente doar pentru camerele active, sub lock-ul lor
        RoomState state = room->state;
        size_t keys = 0;
        if (state == RoomState::Active) {
            std::lock_guard lock(room->room_mutex);
            state = room->state;
            keys = room->keys.size();
        }
        static constexpr const char* STATE_NAMES[] = {"sleeping", "loading", "active", "hibernating"};
        return "*6\r\n" + bulk_string("state") + bulk_string(STATE_NAMES[static_cast<int>(state)]) +
               bulk_string("last_access_ms") + ":" + std::to_string(room->last_access_time) + "\r\n" +
               bulk_string("keys") + ":" + std::to_string(keys) + "\r\n";
    }

    return "-ERR unknown command\r\n";
}

std::string Database::execute_in_room(const std::string& room_name, const std::vector<std::string>& args) {
    if (args.empty()) return "-ERR empty command\r\n";

    std::string command = args[0];
    std::ranges::transform(command, command.begin(), ::toupper);

    const std::shared_ptr<Room> room = find_room(room_name, true);
    // camera adormita se trezeste sub lock-ul ei, nu sub lock-ul global
    std::lock_guard room_lock(room->room_mutex);
    if (!activate(*room)) return ROOMS_FULL;

    if (command == "GET") return handle_get(room_name, *room, args);
    if (command == "SET") return handle_set(room_name, *room, args);
    if (command == "DEL") return handle_del(room_name, *room, args);
    if (command == "CRDTMERGE") return handle_crdtmerge(room_name, *room, args);
    if (command == "TTL") return handle_ttl(room_name, *room, args, false);
    if (command == "PTTL") return handle_ttl(room_name, *room, args, true);
    if (command == "INFO") return handle_info();
    if (command == "SAVE") {
        // nu confirmam niciodata o persistenta suspectata
        return aof.healthy() ? "+OK AOF is active and up to date\r\n"
                             : "-ERR AOF write error, persistenta suspecta\r\n";
    }

    return "-ERR unknown command\r\n";
}

Record* Database::lookup(const std::string& room_name, Room& room, const std::string& key) {
    const long long now = get_current_time_ms();
    if (const auto it = room.keys.find(key); it != room.keys.end()) {
        if (!it->second->expired(now)) {
            global_metrics.cache_hits.fetch_add(1, std::memory_order_relaxed);
            return it->second;
        }
        record_pool.destroy(it->second);
        room.keys.erase(it);
        global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
        return nullptr;
    }

    global_metrics.cache_misses.fetch_add(1, std::memory_order_relaxed);
    std::optional<Record> cold = eviction.take(room_name, key);
    if (!cold || cold->expired(now)) return nullptr;

    Record* r = record_pool.construct(std::move(*cold));
    room.keys.emplace(key, r);
    global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
    return r;
}

std::string Database::handle_get(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() != 2) {
        return "-ERR Wrong number of arguments for GET\r\n";
    }

    global_metrics.total_gets.fetch_add(1, std::memory_order_relaxed);
    const Record* r = lookup(room_name, room, args[1]);
    if (!r) return "$-1\r\n";

    eviction.record_access(args[1]);
    return bulk_string(r->value);
}

// SET key value [NX|XX] [EX seconds|PX milliseconds]
std::string Database::handle_set(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return "-ERR Wrong number of arguments for SET\r\n";
    }

    // validare completa inainte de orice efect: o comanda invalida nu atinge nimic
    const long long now_ms = get_current_time_ms();
    bool nx = false;
    bool xx = false;
    long long expire_at = 0; // termen absolut; 0 = persistent
    for (size_t i = 3; i < args.size(); ++i) {
        std::string opt = args[i];
        std::ranges::transform(opt, opt.begin(), ::toupper);
        if ((opt == "NX" || opt == "XX") && !nx && !xx) {
            nx = opt == "NX";
            xx = opt == "XX";
        } else if ((opt == "EX" || opt == "PX") && expire_at == 0 && i + 1 < args.size()) {
            const long long scale = opt == "EX" ? 1000 : 1;
            long long n = 0;
            if (!fsutil::parse_i64(args[++i], &n) || n <= 0 || n > (LLONG_MAX - now_ms) / scale) {
                return "-ERR invalid expire time in 'set' command\r\n";
            }
            expire_at = now_ms + n * scale;
        } else {
            return "-ERR syntax error\r\n";
        }
    }

    const std::string& key = args[1];
    global_metrics.total_sets.fetch_add(1, std::memory_order_relaxed);

    // conditia vede starea logica (cold storage inclus, expiratele absente);
    // o conditie esuata nu produce nicio mutatie, deci nimic in AOF
    Record* r = lookup(room_name, room, key);
    if ((nx && r) || (xx && !r)) return "$-1\r\n";

    if (!r) {
        r = record_pool.construct();
        room.keys.emplace(key, r);
        global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
    }
    r->value = args[2];
    r->expire_at = expire_at; // un SET simplu sterge si TTL-ul vechi
    r->timestamp_ms = now_ms;
    r->node_id = LOCAL_NODE_ID;

    eviction.record_access(key);
    // mutatia rezultata pleaca in AOF cu termenul absolut si versiunea CRDT:
    // replay-ul o aplica verbatim, fara sa re-evalueze NX/XX sau sa reporneasca TTL-ul
    aof.append(room_name, args, expire_at, now_ms, LOCAL_NODE_ID);
    eviction.evict_despised_keys(room, record_pool);

    return "+OK\r\n";
}

// TTL/PTTL: -2 cheie absenta, -1 cheie persistenta
std::string Database::handle_ttl(const std::string& room_name, Room& room, const std::vector<std::string>& args,
                                 const bool millis) {
    if (args.size() != 2) {
        return "-ERR Wrong number of arguments for " + args[0] + "\r\n";
    }

    const Record* r = lookup(room_name, room, args[1]);
    if (!r) return ":-2\r\n";
    if (r->expire_at == 0) return ":-1\r\n";

    const long long left = std::max(0LL, r->expire_at - get_current_time_ms());
    return ":" + std::to_string(millis ? left : (left + 500) / 1000) + "\r\n";
}

std::string Database::handle_del(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return "-ERR Wrong number of arguments for DEL\r\n";
    }

    // lookup aduce si cheile din cold storage: stergerea le scoate din ambele
    const std::string& key = args[1];
    if (!lookup(room_name, room, key)) return ":0\r\n";

    const auto it = room.keys.find(key);
    record_pool.destroy(it->second);
    room.keys.erase(it);
    global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
    aof.append(room_name, args, 0, get_current_time_ms(), LOCAL_NODE_ID);
    return ":1\r\n";
}

std::string Database::handle_crdtmerge(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 5) {
        return "-ERR Wrong number of arguments for CRDTMERGE\r\n";
    }

    // aceeasi parsare stricta ca la replay: altfel o valoare acceptata live
    // (ex. "-1" sau un nod peste 32 de biti) ar fi respinsa dupa restart
    uint64_t incoming_ts = 0;
    uint64_t incoming_node = 0;
    if (!fsutil::parse_u64(args[3], &incoming_ts) || !fsutil::parse_u64(args[4], &incoming_node) ||
        incoming_node > UINT32_MAX) {
        return "-ERR invalid timestamp or node id for CRDTMERGE\r\n";
    }

    const std::string& key = args[1];
    Record* r = lookup(room_name, room, key);
    if (!r) {
        r = record_pool.construct();
        room.keys.emplace(key, r);
        global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
    } else if (std::tie(incoming_ts, incoming_node) <= std::tie(r->timestamp_ms, r->node_id)) {
        // Monotonic Join Semi-Lattice: actualizam doar daca starea e strict mai noua
        return "+OK (Ignored Stale Write)\r\n";
    }

    r->value = args[2];
    r->timestamp_ms = incoming_ts;
    r->node_id = static_cast<uint32_t>(incoming_node);
    eviction.record_access(key);
    aof.append(room_name, args, r->expire_at, incoming_ts, r->node_id);
    return "+OK (State Converged)\r\n";
}

std::string Database::handle_info() {
    const auto now = std::chrono::steady_clock::now();
    const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();

    // doar contoare atomice: INFO nu ia lock-ul altor camere (ar inversa
    // ordinea rooms_mutex -> room_mutex, camera curenta fiind deja prinsa)
    const std::string info_text =
        "Uptime: " + std::to_string(uptime) + "s\n" +
        "Camere active: " + std::to_string(active_rooms.load()) + "\n" +
        "Chei totale: " + std::to_string(global_metrics.keys_in_ram.load(std::memory_order_relaxed)) + "\n" +
        "Comenzi procesate: " + std::to_string(total_commands.load(std::memory_order_relaxed)) + "\n" +
        "AOF: " + std::string(aof.policy_name()) + (aof.healthy() ? " (sanatos)" : " (ERORI SCRIERE)") + "\n";

    return bulk_string(info_text);
}

void Database::cleanup_client(const int client_fd) {
    pubsub.remove_client(client_fd);

    std::lock_guard lock(client_mutex);
    client_rooms.erase(client_fd);
}

void Database::hibernate_inactive_rooms() {
    std::vector<std::shared_ptr<Room>> candidates;
    const long long now = get_current_time_ms();

    {
        std::shared_lock shared_rooms_lock(rooms_mutex);
        for (const auto& [room_name, room] : rooms) {
            if (room->state == RoomState::Active && room_name != "default" &&
                now - room->last_access_time > ROOM_IDLE_MS) {
                candidates.push_back(room);
            }
        }
    }
    // lock-ul global e liber de aici: I/O-ul de snapshot blocheaza doar camera respectiva

    for (const std::shared_ptr<Room>& room : candidates) {
        std::lock_guard room_lock(room->room_mutex);

        // re-verificam sub lock: o comanda poate a ajuns la camera intre timp
        if (room->state != RoomState::Active || get_current_time_ms() - room->last_access_time <= ROOM_IDLE_MS) {
            continue;
        }
        if (!hibernate(*room)) {
            printf("Hibernare esuata pentru camera '%s': persistenta nereusita, camera ramane activa.\n",
                   room->name.c_str());
        }
    }
}

void Database::clean_expired_keys() {
    std::shared_lock shared_rooms_lock(rooms_mutex);
    const long long now = get_current_time_ms();

    for (const auto& [name, room] : rooms) {
        std::lock_guard room_lock(room->room_mutex);
        for (auto it = room->keys.begin(); it != room->keys.end();) {
            if (it->second->expired(now)) {
                record_pool.destroy(it->second);
                it = room->keys.erase(it);
                global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
            } else {
                ++it;
            }
        }
    }
}
