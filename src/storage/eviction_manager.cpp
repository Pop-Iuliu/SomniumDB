#include "eviction_manager.h"
#include "fs_util.h"
#include "../../metrics.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <ranges>
#include <fcntl.h>
#include <unistd.h>

namespace {
    constexpr char kColdPath[] = "cold.bin";
    constexpr char kColdTmp[] = "cold.bin.tmp";

    // inregistrare: expire_at(i64) | timestamp_ms(u64) | node_id(u32) | valoare
    // (lungimea valorii rezulta din lungimea tinuta in index)
    constexpr size_t kHeader = sizeof(long long) + sizeof(uint64_t) + sizeof(uint32_t);

    std::string encode(const Record& r) {
        std::string out(kHeader, '\0');
        std::memcpy(out.data(), &r.expire_at, sizeof(r.expire_at));
        std::memcpy(out.data() + 8, &r.timestamp_ms, sizeof(r.timestamp_ms));
        std::memcpy(out.data() + 16, &r.node_id, sizeof(r.node_id));
        out += r.value;
        return out;
    }

    Record decode(const std::string& raw) {
        Record r(raw.substr(kHeader), 0);
        std::memcpy(&r.expire_at, raw.data(), sizeof(r.expire_at));
        std::memcpy(&r.timestamp_ms, raw.data() + 8, sizeof(r.timestamp_ms));
        std::memcpy(&r.node_id, raw.data() + 16, sizeof(r.node_id));
        return r;
    }
} // namespace

EvictionManager::EvictionManager() {
    if (const char* env = getenv("SOMNIUM_MAX_KEYS"); env && !fsutil::parse_u64(env, &max_keys_per_room)) {
        fprintf(stderr, "SOMNIUM_MAX_KEYS invalid: '%s' (folosesc %llu)\n", env,
                static_cast<unsigned long long>(max_keys_per_room));
    }
    fd = ::open(kColdPath, O_RDWR | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (fd < 0) {
        fprintf(stderr, "Cold storage indisponibil (%s): cheile raman in RAM\n", strerror(errno));
    }
    publish_metrics();
}

EvictionManager::~EvictionManager() {
    if (fd >= 0) ::close(fd);
}

void EvictionManager::publish_metrics() const {
    global_metrics.cold_file_bytes.store(file_bytes, std::memory_order_relaxed);
    global_metrics.cold_obsolete_bytes.store(obsolete_bytes, std::memory_order_relaxed);
}

void EvictionManager::evict_despised_keys(Room& room, PoolAllocator<Record, 1024>& pool) {
    constexpr int SAMPLE_SIZE = 5;
    static std::mt19937 rng(std::random_device{}());

    while (room.keys.size() > max_keys_per_room) {
        // LFU aproximativ: cea mai rara dintre cateva chei esantionate
        const std::string* victim = nullptr;
        uint16_t lowest = 0xFFFF;
        std::uniform_int_distribution<size_t> dist(0, room.keys.bucket_count() - 1);
        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            size_t bucket = dist(rng);
            while (room.keys.bucket_size(bucket) == 0) bucket = (bucket + 1) % room.keys.bucket_count();
            const std::string& candidate = room.keys.begin(bucket)->first;
            if (const uint16_t freq = cms.estimate_frequency(candidate); !victim || freq < lowest) {
                lowest = freq;
                victim = &candidate;
            }
        }
        // esantionul a prins doar chei fierbinti: cautam explicit una rece
        if (lowest > 10) {
            for (const auto& k : room.keys | std::views::keys) {
                if (cms.estimate_frequency(k) <= 2) {
                    victim = &k;
                    break;
                }
            }
        }

        const auto it = room.keys.find(*victim);
        const std::string data = encode(*it->second);
        if (fd < 0 || !fsutil::write_all(fd, data.data(), data.size())) {
            fprintf(stderr, "Cold storage: scriere esuata, cheile raman in RAM\n");
            break;
        }

        index[room.name][it->first] = {file_bytes, data.size()};
        file_bytes += data.size();
        global_metrics.keys_evicted.fetch_add(1, std::memory_order_relaxed);
        global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);
        pool.destroy(it->second);
        room.keys.erase(it);
    }
    publish_metrics();
}

std::optional<Record> EvictionManager::take(const std::string& room_name, const std::string& key) {
    const auto room_it = index.find(room_name);
    if (room_it == index.end()) return std::nullopt;
    const auto it = room_it->second.find(key);
    if (it == room_it->second.end()) return std::nullopt;

    const ColdRef ref = it->second;
    std::string raw(ref.len, '\0');
    if (!fsutil::pread_all(fd, raw.data(), raw.size(), static_cast<off_t>(ref.off))) {
        // ponytail: o eroare de citire raporteaza cheia ca absenta; intrarea
        // ramane in index ca datele sa nu se piarda
        fprintf(stderr, "Cold storage: citire esuata: %s\n", strerror(errno));
        return std::nullopt;
    }

    room_it->second.erase(it);
    if (room_it->second.empty()) index.erase(room_it);
    obsolete_bytes += ref.len;
    publish_metrics();
    return decode(raw);
}

bool EvictionManager::for_each(
    const std::function<void(const std::string& room, const std::string& key, const Record&)>& fn) const {
    std::string raw;
    for (const auto& [room_name, keys] : index) {
        for (const auto& [key, ref] : keys) {
            raw.resize(ref.len);
            if (!fsutil::pread_all(fd, raw.data(), raw.size(), static_cast<off_t>(ref.off))) return false;
            fn(room_name, key, decode(raw));
        }
    }
    return true;
}

long long EvictionManager::compact(const long long now_ms) {
    const int out = ::open(kColdTmp, O_RDWR | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (out < 0) {
        fprintf(stderr, "Compactare: nu pot crea %s: %s\n", kColdTmp, strerror(errno));
        return -1;
    }

    decltype(index) fresh;
    uint64_t size = 0;
    bool ok = true;
    std::string raw;
    for (const auto& [room_name, keys] : index) {
        for (const auto& [key, ref] : keys) {
            raw.resize(ref.len);
            ok = fsutil::pread_all(fd, raw.data(), raw.size(), static_cast<off_t>(ref.off));
            if (ok && decode(raw).expired(now_ms)) continue; // expirata: recuperata
            ok = ok && fsutil::write_all(out, raw.data(), raw.size());
            if (!ok) break;
            fresh[room_name][key] = {size, ref.len};
            size += ref.len;
        }
        if (!ok) break;
    }

    if (!ok || ::rename(kColdTmp, kColdPath) != 0) {
        fprintf(stderr, "Compactare esuata: %s (fisierul vechi ramane in uz)\n", strerror(errno));
        ::close(out);
        ::unlink(kColdTmp);
        return -1;
    }

    ::close(fd);
    fd = out;
    const uint64_t reclaimed = file_bytes - size;
    index = std::move(fresh);
    file_bytes = size;
    obsolete_bytes = 0;
    global_metrics.cold_reclaimed_bytes.fetch_add(reclaimed, std::memory_order_relaxed);
    publish_metrics();
    return static_cast<long long>(reclaimed);
}
