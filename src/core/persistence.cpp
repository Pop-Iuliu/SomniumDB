// Persistenta: replay-ul AOF la pornire si rescrierea lui (sincrona sau in fundal, S14).
// dump_state() ia registrul si fiecare camera (lock = true); copilul fork-ului
// nu ia niciun lock, fiindca alt thread le putea tine in momentul fork-ului.
#include "database.h"
#include "../storage/fs_util.h"
#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdlib>
#include <sys/wait.h>
#include <unistd.h>

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
