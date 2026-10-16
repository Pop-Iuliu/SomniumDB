// Dispecerarea comenzilor, camera fiecarui client si accesul la inregistrari
// (lookup/insert/set_value/erase: singurele care schimba RAM-ul unei camere,
// cu room_mutex prins). Contractul de lock-uri: room.h.
#include "database.h"
#include "../../metrics.h"
#include "../storage/fs_util.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <climits>

// SOMNIUM_CLOCK_OFFSET_MS muta ceasul: ceas controlabil pentru testele
// de expirare peste restart (timp "trecut" cat serverul era oprit)
long long Database::get_current_time_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count() + clock_offset_ms_;
}

Database::Database(const Config& cfg)
    : local_node_id(cfg.node_id),
      peer_count_(cfg.peers.size()), // aceeasi lista ca replicatorul (REF-2)
      max_clock_offset_ms(cfg.max_clock_offset_ms),
      max_rooms_(cfg.max_rooms),
      clock_offset_ms_(cfg.clock_offset_ms),
      snapshot_delay_ms_(cfg.snapshot_delay_ms),
      aof(cfg),
      eviction(cfg) {
    start_time = std::chrono::steady_clock::now();

    // 1) redare istoric (snapshot-urile se incarca sub prima mutatie a fiecarei camere,
    //    apoi mutatiile AOF se aplica peste ele: un snapshot vechi nu poate
    //    suprascrie starea mai noua redata din AOF)
    aof.recover([this](const AofRecord& rec) {
        this->replay_record(rec);
    });

    // 2) migrare explicita: AOF-urile vechi (v1/v2) sau corupte ajung in v3
    if (aof.needs_rewrite()) {
        rewrite_aof();
    }

    // 3) abia acum deschidem append-ul; fisier nou primeste header v3
    if (!aof.open_file()) {
        fprintf(stderr, "AOF: serverul ruleaza FARA persistenta (deschiderea a esuat)!\n");
    }
    frontier_ = clock_.next(get_current_time_ms(), 0); // tot istoricul redat e deja in AOF
}

Database::~Database() = default;

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

std::string Database::execute(const int client_fd, const std::vector<std::string>& args, const bool resp3) {
    if (args.empty()) return "";
    resp3_ = resp3;
    // aici nu e prins niciun lock: rescrierea ia singura registrul si camerele
    finish_background_rewrite();
    if (rewrite_child_ == 0 && aof.rewrite_due()) start_background_rewrite();

    const std::string current_room = get_client_room(client_fd);
    std::string command = args[0];
    std::ranges::transform(command, command.begin(), ::toupper);

    if (command == "SUBSCRIBE") {
        if (args.size() < 2) return "-ERR Wrong number of arguments for SUBSCRIBE\r\n";
        std::string replies;
        for (size_t i = 1; i < args.size(); ++i) replies += pubsub.subscribe(client_fd, args[i], resp3);
        return replies;
    }

    if (command == "UNSUBSCRIBE") {
        return pubsub.unsubscribe(client_fd, {args.begin() + 1, args.end()}, resp3);
    }

    if (command == "PING") {
        if (args.size() > 2) return "-ERR Wrong number of arguments for PING\r\n";
        // in modul subscribe pe RESP2, ca Redis: raspuns sub forma de mesaj ["pong", payload]
        if (!resp3 && pubsub.is_subscribed(client_fd)) {
            return "*2\r\n$4\r\npong\r\n" + bulk_string(args.size() == 2 ? args[1] : "");
        }
        return args.size() == 2 ? bulk_string(args[1]) : "+PONG\r\n";
    }

    if (command == "PUBLISH") {
        if (args.size() >= 3) return pubsub.publish(args[1], args[2]);
        return "-ERR Wrong number of arguments for PUBLISH\r\n";
    }

    // pe RESP3 mesajele push se disting de raspunsuri, deci orice comanda e permisa
    if (!resp3 && pubsub.is_subscribed(client_fd)) {
        return "-ERR Clientul este in mod SUBSCRIBE. Nu poti trimite comenzi de Database!\r\n";
    }

    total_commands.fetch_add(1, std::memory_order_relaxed);

    if (command == "COMMAND") {
        return "*0\r\n";
    }

    if (command == "ECHO") {
        return args.size() == 2 ? bulk_string(args[1]) : "-ERR Wrong number of arguments for ECHO\r\n";
    }

    // doar contoare globale: INFO nu trezeste camera clientului
    if (command == "INFO") return handle_info();

    // ROOM selecteaza (si trezeste); comenzile de administrare au prefixul
    // "ROOM." ca nume precum LIST sa ramana camere valide
    if (command == "ROOM") {
        if (args.size() != 2) return "-ERR Wrong number of arguments for ROOM\r\n";
        if (const char* err = wake_room(args[1])) return err;
        set_client_room(client_fd, args[1]);
        return "+OK\r\n";
    }

    if (command == "ROOMS" || command.starts_with("ROOM.")) {
        return handle_room_admin(command, args);
    }

    // REPLFRONTIER node F: "toate scrierile mele <= F ti-au fost livrate" (S13)
    if (command == "REPLFRONTIER") {
        uint64_t node = 0, f = 0;
        if (args.size() != 3 || !fsutil::parse_u64(args[1], &node) || node > UINT32_MAX ||
            !fsutil::parse_u64(args[2], &f)) {
            return "-ERR usage: REPLFRONTIER node frontier\r\n";
        }
        uint64_t& known = frontiers_[static_cast<uint32_t>(node)];
        known = std::max(known, f);
        // intre comenzi nu e nimic in zbor: orice versiune locala emisa pana aici
        // e deja in AOF, deci o versiune noua e o frontiera exacta (si avanseaza
        // cu timpul fizic chiar daca nodul nu scrie nimic)
        frontier_ = clock_.next(get_current_time_ms(), 0);
        collect_tombstones();
        return "+OK\r\n";
    }

    if (command == "REWRITEAOF") {
        return start_background_rewrite();
    }

    if (command == "COMPACT") {
        const long long reclaimed = eviction.compact(get_current_time_ms());
        return reclaimed < 0 ? "-ERR compaction failed, previous cold file kept\r\n"
                             : ":" + std::to_string(reclaimed) + "\r\n";
    }

    return execute_in_room(current_room, args);
}

Record* Database::lookup(const std::string& room_name, Room& room, const std::string& key) {
    const long long now = get_current_time_ms();
    if (const auto it = room.keys.find(key); it != room.keys.end()) {
        if (!it->second->expired(now)) {
            global_metrics.cache_hits.fetch_add(1, std::memory_order_relaxed);
            return it->second;
        }
        erase(room, it);
        return nullptr;
    }

    global_metrics.cache_misses.fetch_add(1, std::memory_order_relaxed);
    std::optional<Record> cold = eviction.take(room_name, key);
    if (!cold || cold->expired(now)) return nullptr;

    Record* r = record_pool.construct(std::move(*cold));
    room.keys.emplace(key, r);
    global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
    room.charge(static_cast<long long>(record_cost(key, *r)));
    return r;
}

Record* Database::insert(Room& room, const std::string& key) {
    Record* r = record_pool.construct();
    room.keys.emplace(key, r);
    global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
    room.charge(static_cast<long long>(record_cost(key, *r)));
    return r;
}

void Database::set_value(Room& room, Record* r, const std::string& value) {
    room.charge(static_cast<long long>(value.size()) - static_cast<long long>(r->value.size()));
    r->value = value;
}

void Database::erase(Room& room, const std::unordered_map<std::string, Record*>::iterator it) {
    room.charge(-static_cast<long long>(record_cost(it->first, *it->second)));
    global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
    record_pool.destroy(it->second);
    room.keys.erase(it);
}

void Database::cleanup_client(const int client_fd) {
    pubsub.remove_client(client_fd);

    std::lock_guard lock(client_mutex);
    client_rooms.erase(client_fd);
}
