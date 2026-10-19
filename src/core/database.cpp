// Dispecerarea comenzilor, camera fiecarui client si accesul la inregistrari
// (lookup/insert/set_value/erase: singurele care schimba RAM-ul unei camere,
// cu room_mutex prins). Contractul de lock-uri: room.h.
#include "database.h"
#include "../server/metrics.h"
#include <algorithm>
#include <cctype>
#include <chrono>

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
    return client_rooms.try_emplace(client_fd, "default").first->second; // o singura cautare
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

    // ROOM.<orice> e o singura familie (handle_room_admin isi alege subcomanda); o
    // comanda necunoscuta ruleaza in camera clientului si raspunde "unknown command"
    static const Command unknown{Command::Scope::Room, false,
                                 [](Database&, const Call&) -> std::string { return "-ERR unknown command\r\n"; }};
    static const std::string room_family = "ROOM.";
    const auto it = kCommands.find(command.starts_with(room_family) ? room_family : command); // fara copie
    const Command& cmd = it != kCommands.end() ? it->second : unknown;

    if (!cmd.pubsub) {
        // pe RESP3 mesajele push se disting de raspunsuri, deci orice comanda e permisa
        if (!resp3 && pubsub.is_subscribed(client_fd)) {
            return "-ERR Clientul este in mod SUBSCRIBE. Nu poti trimite comenzi de Database!\r\n";
        }
        bump(total_commands);
    }

    const Call call{client_fd, command, args, current_room, nullptr};
    return cmd.scope == Command::Scope::Room ? execute_in_room(cmd, call) : cmd.run(*this, call);
}

// REF-4: fiecare comanda, cu proprietatile ei langa nume. Numarul de argumente si
// textele de eroare raman in handler-e (decizia D4). Replay-ul AOF are dispecerarea
// lui (replay_record): aplica stare salvata, nu ruleaza comenzi.
const std::unordered_map<std::string, Database::Command> Database::kCommands = [] {
    using S = Command::Scope;
    return std::unordered_map<std::string, Command>{
        // Pub/Sub
        {"SUBSCRIBE", {S::Connection, true, [](Database& db, const Call& c) { return db.handle_subscribe(c.client_fd, c.args); }}},
        {"UNSUBSCRIBE", {S::Connection, true, [](Database& db, const Call& c) {
             return db.pubsub.unsubscribe(c.client_fd, {c.args.begin() + 1, c.args.end()}, db.resp3_); }}},
        {"PING", {S::Connection, true, [](Database& db, const Call& c) { return db.handle_ping(c.client_fd, c.args); }}},
        // mesajul pleaca spre abonati acum, in mijlocul batch-ului: scrierile de dinainte intai in AOF
        {"PUBLISH", {S::Connection, true, [](Database& db, const Call& c) -> std::string {
             if (c.args.size() < 3) return "-ERR Wrong number of arguments for PUBLISH\r\n";
             db.aof.flush();
             return db.pubsub.publish(c.args[1], c.args[2]); }}},

        // conexiunea si serverul, fara camera
        {"COMMAND", {S::Connection, false, [](Database&, const Call&) -> std::string { return "*0\r\n"; }}},
        {"ECHO", {S::Connection, false, [](Database&, const Call& c) -> std::string {
             return c.args.size() == 2 ? bulk_string(c.args[1]) : "-ERR Wrong number of arguments for ECHO\r\n"; }}},
        {"INFO", {S::Connection, false, [](Database& db, const Call&) { return db.handle_info(); }}}, // nu trezeste camera
        {"ROOM", {S::Connection, false, [](Database& db, const Call& c) { return db.handle_room(c.client_fd, c.args); }}},
        {"ROOMS", {S::Connection, false, [](Database& db, const Call& c) { return db.handle_room_admin(c.command, c.args); }}},
        {"ROOM.", {S::Connection, false, [](Database& db, const Call& c) { return db.handle_room_admin(c.command, c.args); }}},
        {"REPLFRONTIER", {S::Connection, false, [](Database& db, const Call& c) { return db.handle_replfrontier(c.args); }}},
        {"REWRITEAOF", {S::Connection, false, [](Database& db, const Call&) { return db.start_background_rewrite(); }}},
        {"COMPACT", {S::Connection, false, [](Database& db, const Call&) -> std::string {
             const long long reclaimed = db.eviction.compact(db.get_current_time_ms());
             return reclaimed < 0 ? "-ERR compaction failed, previous cold file kept\r\n" : ":" + std::to_string(reclaimed) + "\r\n"; }}},

        // chei, in camera clientului
        {"GET", {S::Room, false, [](Database& db, const Call& c) { return db.handle_get(c.room_name, *c.room, c.args); }}},
        {"SET", {S::Room, false, [](Database& db, const Call& c) { return db.handle_set(c.room_name, *c.room, c.args); }}},
        {"DEL", {S::Room, false, [](Database& db, const Call& c) { return db.handle_del(c.room_name, *c.room, c.args); }}},
        {"CRDTMERGE", {S::Room, false, [](Database& db, const Call& c) { return db.handle_crdtmerge(c.room_name, *c.room, c.args); }}},
        {"CRDTDEL", {S::Room, false, [](Database& db, const Call& c) { return db.handle_crdtdel(c.room_name, *c.room, c.args); }}},
        {"MGET", {S::Room, false, [](Database& db, const Call& c) { return db.handle_mget(c.room_name, *c.room, c.args); }}},
        {"MSET", {S::Room, false, [](Database& db, const Call& c) { return db.handle_mset(c.room_name, *c.room, c.args); }}},
        {"EXISTS", {S::Room, false, [](Database& db, const Call& c) { return db.handle_exists(c.room_name, *c.room, c.args); }}},
        {"EXPIRE", {S::Room, false, [](Database& db, const Call& c) { return db.handle_expire(c.room_name, *c.room, c.args, 1000); }}},
        {"PEXPIRE", {S::Room, false, [](Database& db, const Call& c) { return db.handle_expire(c.room_name, *c.room, c.args, 1); }}},
        {"PERSIST", {S::Room, false, [](Database& db, const Call& c) { return db.handle_expire(c.room_name, *c.room, c.args, 0); }}},
        {"TTL", {S::Room, false, [](Database& db, const Call& c) { return db.handle_ttl(c.room_name, *c.room, c.args, false); }}},
        {"PTTL", {S::Room, false, [](Database& db, const Call& c) { return db.handle_ttl(c.room_name, *c.room, c.args, true); }}},
        {"SAVE", {S::Room, false, [](Database& db, const Call&) -> std::string { // nu confirmam o persistenta suspectata
             return db.aof.flush() && db.aof.healthy() ? "+OK AOF is active and up to date\r\n"
                                                       : "-ERR AOF write error, persistenta suspecta\r\n"; }}},
    };
}();

std::string Database::handle_subscribe(const int client_fd, const std::vector<std::string>& args) {
    if (args.size() < 2) return "-ERR Wrong number of arguments for SUBSCRIBE\r\n";
    std::string replies;
    for (size_t i = 1; i < args.size(); ++i) replies += pubsub.subscribe(client_fd, args[i], resp3_);
    return replies;
}

std::string Database::handle_ping(const int client_fd, const std::vector<std::string>& args) {
    if (args.size() > 2) return "-ERR Wrong number of arguments for PING\r\n";
    // in modul subscribe pe RESP2, ca Redis: raspuns sub forma de mesaj ["pong", payload]
    if (!resp3_ && pubsub.is_subscribed(client_fd)) {
        return "*2\r\n$4\r\npong\r\n" + bulk_string(args.size() == 2 ? args[1] : "");
    }
    return args.size() == 2 ? bulk_string(args[1]) : "+PONG\r\n";
}

Record* Database::lookup(const std::string& room_name, Room& room, const std::string& key) {
    const long long now = get_current_time_ms();
    if (const auto it = room.keys.find(key); it != room.keys.end()) {
        if (!it->second->expired(now)) {
            bump(global_metrics.cache_hits);
            return it->second;
        }
        erase(room, it);
        return nullptr;
    }

    bump(global_metrics.cache_misses);
    std::optional<Record> cold = eviction.take(room_name, key);
    if (!cold || cold->expired(now)) return nullptr;

    Record* r = record_pool.construct(std::move(*cold));
    room.keys.emplace(key, r);
    global_metrics.keys_in_ram.fetch_add(1, std::memory_order_relaxed);
    room.charge(static_cast<long long>(record_cost(key, *r)));
    return r;
}

// Ca prefetch-ul pe batch din Redis 8: o cautare atinge in lant bucketul, nodul
// dinaintea lui, nodul, octetii cheii, Record-ul si valoarea, fiecare un miss care
// il asteapta pe precedentul. Pe etape, pentru toate comenzile odata, miss-urile
// comenzilor diferite se suprapun. Doar citiri, sub lock-ul camerei; nimic nu se
// modifica intre etape, deci iteratorii raman valizi.
void Database::prefetch(const int client_fd, const std::vector<std::string>* cmds, const size_t n) {
    constexpr size_t kMax = 16;
    // o camera mica sta oricum in cache: acolo prefetch-ul doar ar costa
    constexpr size_t kMinKeys = 16384;
    const std::string room_name = get_client_room(client_fd);
    const std::shared_ptr<Room> room =
        last_room_ && last_room_->name == room_name ? last_room_ : find_room(room_name, false);
    if (!room) return;
    std::unique_lock lock(room->room_mutex, std::try_to_lock); // nu asteptam watchdog-ul
    if (!lock.owns_lock() || room->state != RoomState::Active || room->keys.size() < kMinKeys) return;

    using Map = std::unordered_map<std::string, Record*>;
    Map& keys = room->keys;
    Map::local_iterator heads[kMax];
    size_t buckets[kMax];
    size_t count = 0;
    // 1: bucketul si primul lui nod (citiri independente intre comenzi)
    for (size_t i = 0; i < n && count < kMax; ++i) {
        if (cmds[i].size() < 2) continue;
        buckets[count] = keys.bucket(cmds[i][1]);
        heads[count] = keys.begin(buckets[count]);
        ++count;
    }
    // 2: nodurile (un bucket gol nu are ce aduce)
    size_t live = 0;
    for (size_t i = 0; i < count; ++i) {
        if (heads[i] == keys.end(buckets[i])) continue;
        __builtin_prefetch(&*heads[i]);
        heads[live++] = heads[i];
    }
    // 3: octetii cheii si Record-ul (de obicei primul nod e chiar cheia cautata).
    // Record-ul si valoarea cu intentie de scriere: SET le suprascrie, iar un store
    // ramas in asteptare ar opri prima instructiune atomica de dupa el (lock-urile)
    for (size_t i = 0; i < live; ++i) {
        __builtin_prefetch(heads[i]->first.data());
        __builtin_prefetch(heads[i]->second, 1);
    }
    // 4: valoarea intreaga (cel mult 4 linii), pe care SET o suprascrie si GET o copiaza
    for (size_t i = 0; i < live; ++i) {
        const std::string& value = heads[i]->second->value;
        const char* p = value.data();
        for (size_t off = 0; off < std::min<size_t>(value.size() + 1, 256); off += 64) __builtin_prefetch(p + off, 1);
    }
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
