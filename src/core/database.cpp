#include "database.h"
#include "../../metrics.h"
#include "../storage/fs_util.h"
#include <algorithm>
#include <cctype>
#include <chrono>

namespace {
    long long get_current_time_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
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
            if (room->hibernated) continue; // camera adormita: snapshot-ul ei este persistenta
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

    // recovery-ul trece peste bugetul de camere active: redarea nu are voie
    // sa piarda mutatii doar pentru ca istoricul are mai multe camere
    const std::shared_ptr<Room> room = get_or_create_room(rec.room, true);
    if (!room) return;

    std::lock_guard room_lock(room->room_mutex);
    room->last_access_time = get_current_time_ms();

    // camera era adormita: snapshot-ul (mai vechi) se incarca acum, sub lock-ul
    // ei, si mutatiile din AOF se aplica peste el in ordinea istorica
    if (room->hibernated) {
        SnapshotManager::wakeup_room(*room, record_pool);
    }

    if (command == "SET" && rec.args.size() >= 3) {
        const std::string& key = rec.args[1];
        const std::string& value = rec.args[2];

        if (const auto it = room->keys.find(key); it != room->keys.end()) {
            Record* r = it->second;
            r->value = value;
            r->expire_at = rec.expire_at;
            r->timestamp_ms = rec.timestamp_ms;
            r->node_id = rec.node_id;
        } else {
            Record* new_rec = record_pool.construct(value, rec.expire_at);
            new_rec->timestamp_ms = rec.timestamp_ms;
            new_rec->node_id = rec.node_id;
            room->keys.emplace(key, new_rec);
            global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
        }
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

    if (command == "CRDTMERGE" && rec.args.size() >= 5) {
        uint64_t incoming_ts = 0;
        uint64_t incoming_node = 0;
        if (!fsutil::parse_u64(rec.args[3], &incoming_ts) ||
            !fsutil::parse_u64(rec.args[4], &incoming_node) ||
            incoming_node > UINT32_MAX) {
            return;
        }
        const std::string& key = rec.args[1];
        const std::string& incoming_val = rec.args[2];
        const auto incoming_state = std::tie(incoming_ts, incoming_node);

        if (const auto it = room->keys.find(key); it != room->keys.end()) {
            Record* r = it->second;
            const auto current_state = std::tie(r->timestamp_ms, r->node_id);
            if (incoming_state > current_state) {
                r->value = incoming_val;
                r->timestamp_ms = incoming_ts;
                r->node_id = static_cast<uint32_t>(incoming_node);
            }
            // stale: ignorat, ca si live
        } else {
            Record* new_rec = record_pool.construct(incoming_val, 0);
            new_rec->timestamp_ms = incoming_ts;
            new_rec->node_id = static_cast<uint32_t>(incoming_node);
            room->keys.emplace(key, new_rec);
            global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
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

std::shared_ptr<Room> Database::get_or_create_room(const std::string& name, const bool allow_over_budget) {
    {
        std::shared_lock lock(rooms_mutex);
        if (const auto it = rooms.find(name); it != rooms.end()) {
            return it->second;
        }
    }

    std::unique_lock lock(rooms_mutex);
    if (const auto it = rooms.find(name); it != rooms.end()) {
        return it->second;
    }

    if (!allow_over_budget) {
        size_t active = 0;
        for (const auto& [room_name, room] : rooms) {
            if (!room->hibernated) active++;
        }
        if (active >= MAX_ACTIVE_ROOMS) {
            return nullptr;
        }
    }

    auto room = std::make_shared<Room>(name);
    // daca exista snapshot pe disk, camera porneste "adormita": prima comanda
    // va apela wakeup_room si va incarca exact o singura data datele
    room->hibernated = SnapshotManager::has_snapshot(name);
    rooms[name] = room;
    lock.unlock();
    return room;
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

    if (command == "ROOM") {
        if (args.size() < 2) return "-ERR Wrong number of arguments for ROOM\r\n";
        if (!get_or_create_room(args[1])) {
            return "-ERR RAM FULL. Te rog asteapta ca o alta camera sa hiberneze!\r\n";
        }
        set_client_room(client_fd, args[1]);
        return "+OK\r\n";
    }

    return execute_in_room(current_room, args);
}

std::string Database::execute_in_room(const std::string& room_name, const std::vector<std::string>& args) {
    if (args.empty()) return "-ERR empty command\r\n";

    std::string command = args[0];
    std::ranges::transform(command, command.begin(), ::toupper);

    std::shared_ptr<Room> room = get_or_create_room(room_name);
    if (!room) {
        return "-ERR RAM FULL. Te rog asteapta ca o alta camera sa hiberneze!\r\n";
    }

    std::lock_guard room_lock(room->room_mutex);
    room->last_access_time = get_current_time_ms();

    // camera era adormita: o trezim sub lock-ul ei, nu sub lock-ul global
    if (room->hibernated) {
        SnapshotManager::wakeup_room(*room, record_pool);
    }

    if (command == "GET") return handle_get(room_name, *room, args);
    if (command == "SET") return handle_set(room_name, *room, args);
    if (command == "DEL") return handle_del(room_name, *room, args);
    if (command == "CRDTMERGE") return handle_crdtmerge(room_name, *room, args);
    if (command == "INFO") return handle_info(*room);
    if (command == "SAVE") {
        // nu confirmam niciodata o persistenta suspectata
        return aof.healthy() ? "+OK AOF is active and up to date\r\n"
                             : "-ERR AOF write error, persistenta suspecta\r\n";
    }

    return "-ERR unknown command\r\n";
}

std::string Database::handle_get(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() != 2) {
        return "-ERR Wrong number of arguments for GET\r\n";
    }

    const std::string& key = args[1];
    global_metrics.total_gets.fetch_add(1, std::memory_order_relaxed);

    const auto it = room.keys.find(key);
    if (it != room.keys.end()) {
        eviction.record_access(key);
        global_metrics.cache_hits.fetch_add(1, std::memory_order_relaxed);

        Record* r = it->second;
        if (r->expire_at > 0 && get_current_time_ms() > r->expire_at) {
            record_pool.destroy(r);
            room.keys.erase(it);
            global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
            return "$-1\r\n";
        }
        return bulk_string(r->value);
    }

    global_metrics.cache_misses.fetch_add(1, std::memory_order_relaxed);

    if (!eviction.possibly_on_disk(key)) {
        global_metrics.bloom_prevented_disk_reads.fetch_add(1, std::memory_order_relaxed);
        return "$-1\r\n";
    }

    std::string cold_val = eviction.read_from_cold_storage(room_name, key);
    if (cold_val.empty()) {
        return "$-1\r\n";
    }

    room.keys[key] = record_pool.construct(cold_val, 0);
    global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
    eviction.record_access(key);

    return bulk_string(cold_val);
}

std::string Database::handle_set(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return "-ERR Wrong number of arguments for SET\r\n";
    }

    const std::string& key = args[1];
    const std::string& value = args[2];
    global_metrics.total_sets.fetch_add(1, std::memory_order_relaxed);

    const long long now_ms = get_current_time_ms();
    const long long expire_at = 0;

    if (const auto it = room.keys.find(key); it != room.keys.end()) {
        Record* r = it->second;
        r->value = value;
        r->expire_at = expire_at;
        r->timestamp_ms = now_ms;
        r->node_id = LOCAL_NODE_ID;
    } else {
        global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
        Record* new_rec = record_pool.construct(value, expire_at);
        new_rec->timestamp_ms = now_ms;
        new_rec->node_id = LOCAL_NODE_ID;
        room.keys[key] = new_rec;
    }

    eviction.record_access(key);
    // mutatia rezultata pleaca in AOF cu metadata ei: expirare (0 = persistent),
    // timestamp CRDT si nod. Replay-ul o aplica verbatim.
    aof.append(room_name, args, expire_at, now_ms, LOCAL_NODE_ID);
    eviction.evict_despised_keys(room, record_pool);

    return "+OK\r\n";
}

std::string Database::handle_del(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return "-ERR Wrong number of arguments for DEL\r\n";
    }

    const std::string& key = args[1];
    if (const auto it = room.keys.find(key); it != room.keys.end()) {
        record_pool.destroy(it->second);
        room.keys.erase(it);
        global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
        // stergerea primeste si ea o versiune (timestamp + nod): baza pentru
        // morminte coerente la evict/reload (S1 le va folosi)
        aof.append(room_name, args, 0, get_current_time_ms(), LOCAL_NODE_ID);
        return ":1\r\n";
    }
    return ":0\r\n";
}

std::string Database::handle_crdtmerge(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 5) {
        return "-ERR Wrong number of arguments for CRDTMERGE\r\n";
    }

    const std::string& key = args[1];
    const std::string& incoming_val = args[2];

    uint64_t incoming_ts = 0;
    uint32_t incoming_node = 0;
    try {
        incoming_ts = std::stoull(args[3]);
        incoming_node = std::stoul(args[4]);
    } catch (const std::exception&) {
        return "-ERR invalid timestamp or node id for CRDTMERGE\r\n";
    }

    const auto it = room.keys.find(key);
    if (it != room.keys.end()) {
        Record* r = it->second;

        const auto current_state = std::tie(r->timestamp_ms, r->node_id);
        const auto incoming_state = std::tie(incoming_ts, incoming_node);

        // Monotonic Join Semi-Lattice: actualizam doar daca starea e strict mai noua
        if (incoming_state > current_state) {
            r->value = incoming_val;
            r->timestamp_ms = incoming_ts;
            r->node_id = incoming_node;
            eviction.record_access(key);
            aof.append(room_name, args, r->expire_at, incoming_ts, incoming_node);
            return "+OK (State Converged)\r\n";
        }
        return "+OK (Ignored Stale Write)\r\n";
    }

    // Cheie noua adusa prin reconciliere
    Record* new_rec = record_pool.construct(incoming_val, 0);
    new_rec->timestamp_ms = incoming_ts;
    new_rec->node_id = incoming_node;

    room.keys[key] = new_rec;
    global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
    eviction.record_access(key);
    aof.append(room_name, args, 0, incoming_ts, incoming_node);

    return "+OK (State Converged)\r\n";
}

std::string Database::handle_info(Room& room) {
    const auto now = std::chrono::steady_clock::now();
    const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();

    size_t total_keys = room.keys.size();
    size_t active_rooms = 0;
    {
        std::shared_lock shared_rooms_lock(rooms_mutex);
        for (const auto& [name, r] : rooms) {
            if (!r->hibernated) active_rooms++;
            if (r.get() == &room) continue;
            std::lock_guard r_lock(r->room_mutex);
            total_keys += r->keys.size();
        }
    }

    const std::string info_text =
        "Uptime: " + std::to_string(uptime) + "s\n" +
        "Camere active: " + std::to_string(active_rooms) + "\n" +
        "Chei totale: " + std::to_string(total_keys) + "\n" +
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
        candidates.reserve(rooms.size());
        for (const auto& [room_name, room] : rooms) {
            if (!room->hibernated && room_name != "default" && now - room->last_access_time > ROOM_IDLE_MS) {
                candidates.push_back(room);
            }
        }
    }
    // lock-ul global e liber de aici: I/O-ul de snapshot blocheaza doar camera respectiva

    for (const std::shared_ptr<Room>& room : candidates) {
        std::lock_guard room_lock(room->room_mutex);

        // re-verificam sub lock: o comanda poate a ajuns la camera intre timp
        if (room->hibernated || get_current_time_ms() - room->last_access_time <= ROOM_IDLE_MS) {
            continue;
        }

        size_t written = 0;
        switch (SnapshotManager::hibernate_room(*room, record_pool, &written)) {
            case HibernateResult::Written:
                break; // snapshot validat; inregistrarile au fost eliberate abia dupa rename
            case HibernateResult::Empty:
                {
                    // coada goala fara date: stergem camera inactiva ca sa nu se acumuleze
                    std::unique_lock exclusive_rooms_lock(rooms_mutex);
                    if (rooms[room->name] == room) {
                        rooms.erase(room->name);
                    }
                }
                break;
            case HibernateResult::Failure:
                printf("Hibernare esuata pentru camera '%s': persistenta nereusita, camera ramane activa.\n",
                       room->name.c_str());
                break;
        }
    }
}

void Database::clean_expired_keys() {
    std::shared_lock shared_rooms_lock(rooms_mutex);
    const long long now = get_current_time_ms();

    for (const auto& [name, room] : rooms) {
        std::lock_guard room_lock(room->room_mutex);
        for (auto it = room->keys.begin(); it != room->keys.end();) {
            if (it->second->expire_at > 0 && it->second->expire_at < now) {
                record_pool.destroy(it->second);
                it = room->keys.erase(it);
                global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
            } else {
                ++it;
            }
        }
    }
}
