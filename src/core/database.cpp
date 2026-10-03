#include "database.h"
#include "../../metrics.h"
#include "../storage/fs_util.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <sys/wait.h>
#include <unistd.h>

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
    if (const char* env = getenv("SOMNIUM_NODE_ID")) {
        uint64_t id = 0;
        if (fsutil::parse_u64(env, &id) && id >= 1 && id <= UINT32_MAX) local_node_id = static_cast<uint32_t>(id);
        else fprintf(stderr, "SOMNIUM_NODE_ID invalid: '%s' (folosesc 1)\n", env);
    }
    if (const char* env = getenv("SOMNIUM_PEERS")) {
        const std::string peers = env;
        for (size_t start = 0; start < peers.size();) {
            const size_t comma = std::min(peers.find(',', start), peers.size());
            peer_count_ += comma > start;
            start = comma + 1;
        }
    }
    if (const char* env = getenv("SOMNIUM_MAX_CLOCK_OFFSET_MS"); env && !fsutil::parse_i64(env, &max_clock_offset_ms)) {
        fprintf(stderr, "SOMNIUM_MAX_CLOCK_OFFSET_MS invalid: '%s'\n", env);
    }

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

// AOF-ul rescris ca stare curenta (si migrarea v1/v2 -> v3, aceeasi operatie):
// camerele active din RAM, cele adormite din snapshot (citit intr-o camera
// temporara, fara trezire si fara buget) si cheile din cold storage. Cheile
// expirate nu se scriu. Temporar -> fsync -> rename: un esec lasa AOF-ul vechi.
// ponytail: sincron pe thread-ul de comenzi; rescriere in fundal cand pauza se masoara
bool Database::rewrite_aof() {
    if (!aof.start_rewrite()) return false;
    discard_active_snapshots();
    if (!dump_state([this](const AofRecord& rec) { return aof.append_rewrite(rec); }, true)) {
        aof.abort_rewrite();
        printf("AOF: rescrierea a esuat; fisierul vechi ramane in uz\n");
        return false;
    }
    return aof.commit_rewrite();
}

// noul AOF nu mai contine DEL-urile: un snapshot ramas de la ultima trezire a unei
// camere active s-ar incarca la replay si ar invia cheile sterse de atunci
void Database::discard_active_snapshots() {
    std::shared_lock rooms_lock(rooms_mutex);
    for (const auto& [name, room] : rooms) {
        std::lock_guard room_lock(room->room_mutex);
        if (room->state != RoomState::Sleeping) SnapshotManager::discard(name);
    }
}

// camerele active din RAM, cele adormite din snapshot (citit intr-o camera
// temporara, fara trezire si fara buget), cheile din cold storage si
// tombstone-urile. Cheile expirate nu se scriu.
bool Database::dump_state(const std::function<bool(const AofRecord&)>& write, const bool lock) {
    const long long now = get_current_time_ms();
    bool ok = true;
    const auto emit = [&](const std::string& room, const std::string& key, const Record& r) {
        if (ok && !r.expired(now)) ok = write({room, {"SET", key, r.value}, r.expire_at, r.timestamp_ms, r.node_id});
    };

    {
        std::shared_lock rooms_lock(rooms_mutex, std::defer_lock);
        if (lock) rooms_lock.lock();
        for (const auto& [name, room] : rooms) {
            std::unique_lock room_lock(room->room_mutex, std::defer_lock);
            if (lock) room_lock.lock();
            if (room->state == RoomState::Sleeping) {
                Room snapshot(name);
                if (SnapshotManager::has_snapshot(name) && !SnapshotManager::wakeup_room(snapshot, record_pool)) {
                    ok = false; // snapshot ilizibil: AOF-ul vechi ramane singura copie
                }
                for (const auto& [key, r] : snapshot.keys) {
                    emit(name, key, *r);
                    record_pool.destroy(r);
                }
            } else {
                for (const auto& [key, r] : room->keys) emit(name, key, *r);
            }
            if (!ok) return false;
        }
    }
    if (ok && !eviction.for_each(emit)) ok = false;
    for (const auto& [room, keys] : tombstones_) {
        for (const auto& [key, v] : keys) {
            if (ok) ok = write({room, {"DEL", key}, 0, v.ts, v.node});
        }
    }
    return ok;
}

std::string Database::start_background_rewrite() {
    if (rewrite_child_ > 0) return "-ERR AOF rewrite already in progress\r\n";
    if (!aof.start_rewrite()) return "-ERR AOF rewrite failed, previous AOF kept\r\n";
    discard_active_snapshots();
    rewrite_from_ = aof.size();

    pid_t pid;
    {
        // instantaneu copy-on-write (Kemper & Neumann, HyPer, ICDE 2011): cu registrul
        // si toate camerele prinse, niciun thread nu e la jumatatea unei modificari
        std::shared_lock rooms_lock(rooms_mutex);
        std::vector<std::unique_lock<std::mutex>> room_locks;
        for (const auto& [name, room] : rooms) room_locks.emplace_back(room->room_mutex);
        pid = fork();
        if (pid == 0) {
            // copilul: fara lacate (raman "prinse" de copia acestui thread), fara stdio
            // si fara mutexul AOF (alt thread le putea tine la fork); doar write()
            const int fd = aof.rewrite_fd();
            std::string buf;
            bool ok = true;
            const auto write = [&](const AofRecord& rec) {
                buf += AOFManager::encode_record(rec);
                if (buf.size() >= (1 << 16)) {
                    ok = fsutil::write_all(fd, buf.data(), buf.size());
                    buf.clear();
                }
                return ok;
            };
            ok = dump_state(write, false) && fsutil::write_all(fd, buf.data(), buf.size()) && fsutil::sync_fd(fd);
            _exit(ok ? 0 : 1);
        }
    }
    if (pid < 0) {
        aof.abort_rewrite();
        return "-ERR AOF rewrite failed (fork), previous AOF kept\r\n";
    }
    rewrite_child_ = pid;
    printf("AOF: rescriere in fundal pornita (pid %d)\n", static_cast<int>(pid));
    return "+Background AOF rewrite started\r\n";
}

void Database::finish_background_rewrite() {
    if (rewrite_child_ <= 0) return;
    int status = 0;
    const pid_t done = waitpid(rewrite_child_, &status, WNOHANG);
    if (done == 0) return;
    rewrite_child_ = 0;

    // instantaneul + coada AOF-ului vechi de la fork: inregistrarile poarta starea
    // rezultata, deci se redau exact la starea curenta, oricand a citit copilul
    const bool child_ok = done > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    if (child_ok && aof.append_tail(rewrite_from_) && aof.commit_rewrite()) return;
    aof.abort_rewrite();
    printf("AOF: rescrierea in fundal a esuat; fisierul vechi ramane in uz\n");
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

    // starea rezultata, aplicata verbatim: valoare, termen absolut si versiune
    const auto put = [&](const std::string& key, const std::string& value, const uint64_t ts, const uint64_t node) {
        const auto it = room->keys.find(key);
        Record* r = it != room->keys.end() ? it->second : insert(*room, key);
        set_value(*room, r, value);
        r->expire_at = rec.expire_at;
        r->timestamp_ms = hlc::normalize(ts);
        r->node_id = static_cast<uint32_t>(node);
        clock_.observe(r->timestamp_ms); // dupa restart, scrierile locale depasesc istoricul
    };

    // live, CRDTMERGE ajunge in AOF doar cand a convers, deci nu se re-evalueaza
    // nici el (live, o cheie expirata conteaza ca absenta; o comparatie la
    // replay ar putea da alt rezultat)
    const bool is_merge = command == "CRDTMERGE" && rec.args.size() >= 5;
    if (is_merge || (command == "SET" && rec.args.size() >= 3)) {
        uint64_t ts = rec.timestamp_ms;
        uint64_t node = rec.node_id;
        // formatele vechi nu au meta: versiunea merge-ului vine din argumente
        if (is_merge && (!fsutil::parse_u64(rec.args[3], &ts) || !fsutil::parse_u64(rec.args[4], &node) ||
                         node > UINT32_MAX)) {
            return;
        }
        put(rec.args[1], rec.args[2], ts, node);
        return;
    }

    // MSET: o singura inregistrare pentru toate perechile (atomica la crash)
    if (command == "MSET") {
        for (size_t i = 1; i + 1 < rec.args.size(); i += 2) {
            put(rec.args[i], rec.args[i + 1], rec.timestamp_ms, rec.node_id);
        }
        return;
    }

    // termenul absolut din meta, nu TTL-ul relativ din argumente: fara viata in plus
    if ((command == "EXPIRE" || command == "PEXPIRE" || command == "PERSIST") && rec.args.size() >= 2) {
        if (const auto it = room->keys.find(rec.args[1]); it != room->keys.end()) {
            it->second->expire_at = rec.expire_at;
        }
        return;
    }

    if ((command == "DEL" && rec.args.size() >= 2) || (command == "CRDTDEL" && rec.args.size() >= 4)) {
        if (const auto it = room->keys.find(rec.args[1]); it != room->keys.end()) erase(*room, it);
        const Version v{hlc::normalize(rec.timestamp_ms), rec.node_id};
        set_tomb(rec.room, rec.args[1], v);
        clock_.observe(v.ts);
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
    const size_t cost = room.keys_cost();
    if (!over_budget && !eviction.fits(cost)) {
        // camera nu incape in SOMNIUM_MAXMEMORY: renuntam la ce am incarcat;
        // snapshot-ul a ramas neatins, deci nu se schimba nimic
        for (const auto& [key, r] : room.keys) record_pool.destroy(r);
        room.keys.clear();
        room.state = RoomState::Sleeping;
        active_rooms.fetch_sub(1);
        return false;
    }
    global_metrics.keys_in_ram.fetch_add(room.keys.size(), std::memory_order_relaxed);
    room.charge(static_cast<long long>(cost));
    room.state = RoomState::Active;
    return true;
}

bool Database::hibernate(Room& room) {
    if (room.state == RoomState::Sleeping) return true;

    room.state = RoomState::Hibernating;
    const size_t resident = room.keys.size();
    const size_t cost = room.keys_cost();
    if (!SnapshotManager::hibernate_room(room, record_pool)) {
        room.state = RoomState::Active;
        return false;
    }
    global_metrics.keys_in_ram.fetch_sub(resident, std::memory_order_relaxed);
    room.charge(-static_cast<long long>(cost));
    room.state = RoomState::Sleeping;
    active_rooms.fetch_sub(1);
    return true;
}

bool Database::wake_room(const std::string& name) {
    const std::shared_ptr<Room> room = find_room(name, true);
    std::lock_guard lock(room->room_mutex);
    return activate(*room);
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
        size_t bytes = 0;
        if (state == RoomState::Active) {
            std::lock_guard lock(room->room_mutex);
            state = room->state;
            keys = room->keys.size();
            bytes = room->bytes;
        }
        static constexpr const char* STATE_NAMES[] = {"sleeping", "loading", "active", "hibernating"};
        return map_header(4) + bulk_string("state") + bulk_string(STATE_NAMES[static_cast<int>(state)]) +
               bulk_string("last_access_ms") + ":" + std::to_string(room->last_access_time) + "\r\n" +
               bulk_string("keys") + ":" + std::to_string(keys) + "\r\n" +
               bulk_string("bytes") + ":" + std::to_string(bytes) + "\r\n";
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

    const std::string reply = [&]() -> std::string {
        if (command == "GET") return handle_get(room_name, *room, args);
        if (command == "SET") return handle_set(room_name, *room, args);
        if (command == "DEL") return handle_del(room_name, *room, args);
        if (command == "CRDTMERGE") return handle_crdtmerge(room_name, *room, args);
        if (command == "CRDTDEL") return handle_crdtdel(room_name, *room, args);
        if (command == "MGET") return handle_mget(room_name, *room, args);
        if (command == "MSET") return handle_mset(room_name, *room, args);
        if (command == "EXISTS") return handle_exists(room_name, *room, args);
        if (command == "EXPIRE") return handle_expire(room_name, *room, args, 1000);
        if (command == "PEXPIRE") return handle_expire(room_name, *room, args, 1);
        if (command == "PERSIST") return handle_expire(room_name, *room, args, 0);
        if (command == "TTL") return handle_ttl(room_name, *room, args, false);
        if (command == "PTTL") return handle_ttl(room_name, *room, args, true);
        if (command == "INFO") return handle_info();
        if (command == "SAVE") {
            // nu confirmam niciodata o persistenta suspectata
            return aof.healthy() ? "+OK AOF is active and up to date\r\n"
                                 : "-ERR AOF write error, persistenta suspecta\r\n";
        }

        return "-ERR unknown command\r\n";
    }();

    // evictare o singura data, dupa comanda: acopera scrierile, dar si cheile
    // reincarcate din cold storage de citiri (raspunsul e deja construit)
    eviction.evict_despised_keys(*room, record_pool);
    return reply;
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

    // lookup aduce si cheile din cold storage: stergerea le scoate din ambele
    const std::string& key = args[1];
    const Record* r = lookup(room_name, room, key);
    if (!r) return ":0\r\n";

    // stergerea e o scriere versionata: peste versiunea cheii, deci castiga pe orice nod
    const Version v{clock_.next(get_current_time_ms(), r->timestamp_ms), local_node_id};
    erase(room, room.keys.find(key));
    set_tomb(room_name, key, v);
    aof.append(room_name, args, 0, v.ts, v.node);
    return ":1\r\n";
}

// CRDTDEL key timestamp node: stergere replicata; pierde fata de o scriere mai noua
std::string Database::handle_crdtdel(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    if (args.size() != 4) {
        return "-ERR Wrong number of arguments for CRDTDEL\r\n";
    }
    Version v;
    if (std::string err = incoming_version(args[2], args[3], &v); !err.empty()) return err;

    const std::string& key = args[1];
    const Record* r = lookup(room_name, room, key);
    if (stale(room_name, key, r, v)) return "+OK (Ignored Stale Write)\r\n";
    if (r) erase(room, room.keys.find(key));
    set_tomb(room_name, key, v);
    aof.append(room_name, args, 0, v.ts, v.node);
    return "+OK (Deleted)\r\n";
}

std::string Database::incoming_version(const std::string& ts_arg, const std::string& node_arg, Version* out) {
    uint64_t ts = 0;
    uint64_t node = 0;
    if (!fsutil::parse_u64(ts_arg, &ts) || !fsutil::parse_u64(node_arg, &node) || node > UINT32_MAX) {
        return "-ERR invalid timestamp or node id\r\n";
    }
    // versiunile in ms (clienti vechi) intra in formatul HLC; o versiune prea
    // departe in viitor ar ingheta cheia pentru totdeauna (nicio scriere n-ar mai castiga)
    ts = hlc::normalize(ts);
    if (static_cast<long long>(hlc::physical_ms(ts)) > get_current_time_ms() + max_clock_offset_ms) {
        return "-ERR timestamp too far in the future (SOMNIUM_MAX_CLOCK_OFFSET_MS)\r\n";
    }
    clock_.observe(ts);
    *out = {ts, static_cast<uint32_t>(node)};
    return "";
}

bool Database::stale(const std::string& room_name, const std::string& key, const Record* r, const Version& v) const {
    if (r) return v <= Version{r->timestamp_ms, r->node_id};
    if (const Version* t = find_tomb(room_name, key)) return v <= *t;
    // nimic local: o scriere sub watermark e prea veche, tombstone-ul care ar fi
    // invins-o putea fi deja uitat (fara asta, cheile sterse ar reinvia)
    return v.ts <= watermark_;
}

const Version* Database::find_tomb(const std::string& room_name, const std::string& key) const {
    const auto room_it = tombstones_.find(room_name);
    if (room_it == tombstones_.end()) return nullptr;
    const auto it = room_it->second.find(key);
    return it == room_it->second.end() ? nullptr : &it->second;
}

uint64_t Database::tomb_ts(const std::string& room_name, const std::string& key) const {
    const Version* t = find_tomb(room_name, key);
    return t ? t->ts : 0;
}

void Database::set_tomb(const std::string& room_name, const std::string& key, const Version& v) {
    // fara replicare nu are cine sa trimita scrieri vechi; sub watermark ar fi uitat oricum
    if (peer_count_ == 0 || v.ts <= watermark_) return;
    const auto [it, inserted] = tombstones_[room_name].try_emplace(key, v);
    if (inserted) global_metrics.tombstones.fetch_add(1, std::memory_order_relaxed);
    else it->second = std::max(it->second, v);
}

void Database::clear_tomb(const std::string& room_name, const std::string& key) {
    const auto room_it = tombstones_.find(room_name);
    if (room_it == tombstones_.end() || room_it->second.erase(key) == 0) return;
    global_metrics.tombstones.fetch_sub(1, std::memory_order_relaxed);
    if (room_it->second.empty()) tombstones_.erase(room_it);
}

// watermark = minimul frontierelor tuturor peer-ilor; un tombstone sub el nu mai
// poate fi contrazis de nicio scriere care inca n-a sosit
// ponytail: parcurgere completa la fiecare avans; un index ordonat dupa versiune daca sunt multe
void Database::collect_tombstones() {
    size_t heard = 0;
    uint64_t low = UINT64_MAX;
    for (const auto& [node, f] : frontiers_) {
        if (node == local_node_id) continue;
        ++heard;
        low = std::min(low, f);
    }
    if (peer_count_ == 0 || heard < peer_count_ || low <= watermark_) return;

    watermark_ = low;
    for (auto room_it = tombstones_.begin(); room_it != tombstones_.end();) {
        auto& keys = room_it->second;
        for (auto it = keys.begin(); it != keys.end();) {
            if (it->second.ts > watermark_) {
                ++it;
                continue;
            }
            it = keys.erase(it);
            global_metrics.tombstones.fetch_sub(1, std::memory_order_relaxed);
        }
        room_it = keys.empty() ? tombstones_.erase(room_it) : std::next(room_it);
    }
}

std::string Database::handle_crdtmerge(const std::string& room_name, Room& room, const std::vector<std::string>& args) {
    // CRDTMERGE key value timestamp node [expire_at]: termenul absolut optional
    // calatoreste cu starea (replicarea S10 trimite si expirarea unui SET ... PX)
    if (args.size() < 5 || args.size() > 6) {
        return "-ERR Wrong number of arguments for CRDTMERGE\r\n";
    }

    // aceeasi parsare stricta ca la replay: altfel o valoare acceptata live
    // (ex. "-1" sau un nod peste 32 de biti) ar fi respinsa dupa restart
    long long expire_at = 0;
    if (args.size() == 6 && (!fsutil::parse_i64(args[5], &expire_at) || expire_at < 0)) {
        return "-ERR invalid expire time for CRDTMERGE\r\n";
    }
    Version v;
    if (std::string err = incoming_version(args[3], args[4], &v); !err.empty()) return err;

    const std::string& key = args[1];
    Record* r = lookup(room_name, room, key);
    // Monotonic Join Semi-Lattice: actualizam doar daca starea e strict mai noua
    if (stale(room_name, key, r, v)) return "+OK (Ignored Stale Write)\r\n";
    if (!r) {
        r = insert(room, key);
        clear_tomb(room_name, key);
    }

    set_value(room, r, args[2]);
    if (args.size() == 6) r->expire_at = expire_at;
    r->timestamp_ms = v.ts;
    r->node_id = v.node;
    eviction.record_access(key);
    aof.append(room_name, args, r->expire_at, v.ts, v.node);
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
        "Octeti rezidenti: " + std::to_string(global_metrics.resident_bytes.load(std::memory_order_relaxed)) + "\n" +
        "Tombstone-uri: " + std::to_string(global_metrics.tombstones.load(std::memory_order_relaxed)) + "\n" +
        "Rescriere AOF: " + (rewrite_child_ > 0 ? "in curs" : "inactiva") + "\n" +
        "Comenzi procesate: " + std::to_string(total_commands.load(std::memory_order_relaxed)) + "\n" +
        "AOF: " + std::string(aof.policy_name()) + (aof.healthy() ? " (sanatos)" : " (ERORI SCRIERE)") + "\n" +
        peers_report(false);

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
                erase(*room, it++);
            } else {
                ++it;
            }
        }
    }
}
