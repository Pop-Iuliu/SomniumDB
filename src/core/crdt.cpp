// Versiuni CRDT si tombstone-uri: merge-uri si stergeri replicate, colectarea (S13).
// Tombstone-urile, frontierele si watermark-ul nu au lock propriu: le atinge doar
// thread-ul de comenzi.
#include "database.h"
#include "../../metrics.h"
#include "../storage/fs_util.h"
#include <algorithm>
#include <climits>

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
