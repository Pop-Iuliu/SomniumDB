// Comenzile pe chei. Toate ruleaza din execute_in_room(), cu room_mutex prins,
// pe thread-ul de comenzi.
#include "database.h"
#include "../server/metrics.h"
#include "../storage/fs_util.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <climits>

std::string Database::execute_in_room(const Command& cmd, const Call& call) {
    const std::shared_ptr<Room> room = find_room(call.room_name, true);
    // camera adormita se trezeste sub lock-ul ei, nu sub lock-ul global
    std::lock_guard room_lock(room->room_mutex);
    if (!activate(*room)) return ROOMS_FULL;

    const std::string reply = cmd.run(*this, {call.client_fd, call.command, call.args, call.room_name, room.get()});

    // evictare o singura data, dupa comanda: acopera scrierile, dar si cheile
    // reincarcate din cold storage de citiri (raspunsul e deja construit)
    eviction.evict_despised_keys(*room, record_pool);
    return reply;
}

std::string Database::handle_get(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() != 2) {
        return "-ERR Wrong number of arguments for GET\r\n";
    }

    global_metrics.total_gets.fetch_add(1, std::memory_order_relaxed);
    const Record* r = lookup(room_name, room, args[1]);
    if (!r) return nil();

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
    if ((nx && r) || (xx && !r)) return nil();
    const uint64_t version = clock_.next(now_ms, r ? r->timestamp_ms : tomb_ts(room_name, key));
    if (!r) {
        r = insert(room, key);
        clear_tomb(room_name, key);
    }
    set_value(room, r, args[2]);
    r->expire_at = expire_at; // un SET simplu sterge si TTL-ul vechi
    r->timestamp_ms = version;
    r->node_id = local_node_id;

    eviction.record_access(key);
    // mutatia rezultata pleaca in AOF cu termenul absolut si versiunea CRDT:
    // replay-ul o aplica verbatim, fara sa re-evalueze NX/XX sau sa reporneasca TTL-ul
    aof.append(room_name, args, expire_at, version, local_node_id);

    return "+OK\r\n";
}

// MSET key value [key value ...]: atomic, o singura inregistrare in AOF
std::string Database::handle_mset(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 3 || args.size() % 2 == 0) {
        return "-ERR Wrong number of arguments for MSET\r\n";
    }

    uint64_t overwritten = 0;
    for (size_t i = 1; i < args.size(); i += 2) {
        const Record* r = lookup(room_name, room, args[i]);
        overwritten = std::max(overwritten, r ? r->timestamp_ms : tomb_ts(room_name, args[i]));
    }
    const uint64_t version = clock_.next(get_current_time_ms(), overwritten);
    for (size_t i = 1; i < args.size(); i += 2) {
        Record* r = lookup(room_name, room, args[i]);
        if (!r) {
            r = insert(room, args[i]);
            clear_tomb(room_name, args[i]);
        }
        set_value(room, r, args[i + 1]);
        r->expire_at = 0; // ca la SET simplu: TTL-ul vechi dispare
        r->timestamp_ms = version;
        r->node_id = local_node_id;
        eviction.record_access(args[i]);
    }
    global_metrics.total_sets.fetch_add(args.size() / 2, std::memory_order_relaxed);

    aof.append(room_name, args, 0, version, local_node_id);
    return "+OK\r\n";
}

std::string Database::handle_mget(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return "-ERR Wrong number of arguments for MGET\r\n";
    }

    std::string out = "*" + std::to_string(args.size() - 1) + "\r\n";
    for (size_t i = 1; i < args.size(); ++i) {
        const Record* r = lookup(room_name, room, args[i]);
        if (!r) {
            out += nil();
            continue;
        }
        // SEC-19: aceeasi cheie mare repetata ar construi gigaocteti dintr-o cerere mica
        if (out.size() + r->value.size() > MAX_REPLY) return "-ERR reply too large\r\n";
        eviction.record_access(args[i]);
        out += bulk_string(r->value);
    }
    global_metrics.total_gets.fetch_add(args.size() - 1, std::memory_order_relaxed);
    return out;
}

// EXISTS key [key ...]: cheile repetate se numara de fiecare data, ca la Redis
std::string Database::handle_exists(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return "-ERR Wrong number of arguments for EXISTS\r\n";
    }

    size_t found = 0;
    for (size_t i = 1; i < args.size(); ++i) found += lookup(room_name, room, args[i]) != nullptr;
    return ":" + std::to_string(found) + "\r\n";
}

// EXPIRE key seconds [NX|XX|GT|LT] (scale 1000), PEXPIRE key ms [...] (scale 1),
// PERSIST key (scale 0). In AOF ajunge termenul absolut, deci replay-ul nu
// reporneste TTL-ul (si nu re-evalueaza optiunile).
std::string Database::handle_expire(const std::string& room_name, Room& room, const std::vector<std::string>& args,
                                    const long long scale) {
    if (scale ? args.size() < 3 : args.size() != 2) {
        return "-ERR Wrong number of arguments for " + args[0] + "\r\n";
    }

    bool nx = false, xx = false, gt = false, lt = false;
    for (size_t i = 3; i < args.size(); ++i) {
        std::string opt = args[i];
        std::ranges::transform(opt, opt.begin(), ::toupper);
        if (opt == "NX") nx = true;
        else if (opt == "XX") xx = true;
        else if (opt == "GT") gt = true;
        else if (opt == "LT") lt = true;
        else return "-ERR Unsupported option " + args[i] + "\r\n";
    }
    if (nx && (xx || gt || lt)) return "-ERR NX and XX, GT or LT options at the same time are not compatible\r\n";
    if (gt && lt) return "-ERR GT and LT options at the same time are not compatible\r\n";

    long long n = 0;
    long long expire_at = 0;
    if (scale) {
        const long long now_ms = get_current_time_ms();
        if (!fsutil::parse_i64(args[2], &n) || n < LLONG_MIN / scale || n > (LLONG_MAX - now_ms) / scale) {
            return "-ERR invalid expire time\r\n";
        }
        expire_at = now_ms + n * scale;
    }

    Record* r = lookup(room_name, room, args[1]);
    if (!r || (!scale && r->expire_at == 0)) return ":0\r\n";

    // ca Redis 7: o cheie fara termen are un TTL infinit, deci GT nu se aplica
    // niciodata, iar LT mereu; optiunile se verifica inainte de stergere
    const bool has_ttl = r->expire_at != 0;
    if ((nx && has_ttl) || (xx && !has_ttl) || (gt && (!has_ttl || expire_at <= r->expire_at)) ||
        (lt && has_ttl && expire_at >= r->expire_at)) {
        return ":0\r\n";
    }

    // un termen care a trecut deja sterge cheia
    if (scale && n <= 0) return handle_del(room_name, room, {"DEL", args[1]});

    // o scriere versionata, cu starea completa: se replica si se reda ca orice SET
    r->expire_at = expire_at;
    r->timestamp_ms = clock_.next(get_current_time_ms(), r->timestamp_ms);
    r->node_id = local_node_id;
    aof.append(room_name, {"SET", args[1], r->value}, expire_at, r->timestamp_ms, r->node_id);
    return ":1\r\n";
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

    // DEL key [key ...], ca la Redis (inainte, cheile de dupa prima erau ignorate in tacere).
    // O inregistrare AOF per cheie: replay-ul si replicarea (CRDTDEL) raman pe o singura cheie
    size_t deleted = 0;
    for (size_t i = 1; i < args.size(); ++i) {
        // lookup aduce si cheile din cold storage: stergerea le scoate din ambele
        const std::string& key = args[i];
        const Record* r = lookup(room_name, room, key);
        if (!r) continue;

        // stergerea e o scriere versionata: peste versiunea cheii, deci castiga pe orice nod
        const Version v{clock_.next(get_current_time_ms(), r->timestamp_ms), local_node_id};
        erase(room, room.keys.find(key));
        set_tomb(room_name, key, v);
        aof.append(room_name, {"DEL", key}, 0, v.ts, v.node);
        ++deleted;
    }
    return ":" + std::to_string(deleted) + "\r\n";
}

std::string Database::handle_info() {
    const auto now = std::chrono::steady_clock::now();
    const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();

    // doar contoare atomice: INFO nu ia lock-ul niciunei camere
    const std::string info_text =
        "Uptime: " + std::to_string(uptime) + "s\n" +
        "Camere active: " + std::to_string(active_rooms.load()) + "\n" +
        "Chei totale: " + std::to_string(global_metrics.keys_in_ram.load(std::memory_order_relaxed)) + "\n" +
        "Octeti rezidenti: " + std::to_string(global_metrics.resident_bytes.load(std::memory_order_relaxed)) + "\n" +
        "Tombstone-uri: " + std::to_string(global_metrics.tombstones.load(std::memory_order_relaxed)) + "\n" +
        "Rescriere AOF: " + (rewrite_child_ > 0 ? "in curs" : "inactiva") + "\n" +
        "Comenzi procesate: " + std::to_string(total_commands.load(std::memory_order_relaxed)) + "\n" +
        "AOF: " + std::string(aof.policy_name()) + (aof.healthy() ? " (sanatos)" : " (ERORI SCRIERE)") + "\n" +
        "AOF sync: age=" + std::to_string(aof_sync_age_ms()) + "ms pending=" +
        std::to_string(global_metrics.aof_pending_bytes.load(std::memory_order_relaxed)) +
        " duration_us=" + std::to_string(global_metrics.aof_sync_duration_us.load(std::memory_order_relaxed)) +
        " overruns=" + std::to_string(global_metrics.aof_sync_overruns.load(std::memory_order_relaxed)) +
        " inflight=" + std::to_string(global_metrics.aof_sync_inflight.load(std::memory_order_relaxed)) + "\n" +
        peers_report(false);

    return bulk_string(info_text);
}

void Database::clean_expired_keys() {
    std::shared_lock shared_rooms_lock(rooms_mutex);
    const long long now = get_current_time_ms();

    for (const auto& [name, room] : rooms) {
        std::lock_guard room_lock(room->room_mutex);
        for (auto it = room->keys.begin(); it != room->keys.end();) {
            if (it->second->expired(now)) {
                erase(*room, it++);
            } else {
                ++it;
            }
        }
    }
}
