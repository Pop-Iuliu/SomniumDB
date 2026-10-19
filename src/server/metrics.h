//
// Created by tiwerlol on 08.08.2026.
//

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

struct DbMetrics {
    std::atomic<uint64_t> keys_in_ram{0};
    std::atomic<uint64_t> resident_bytes{0}; // estimare, vezi record_cost()
    std::atomic<uint64_t> tombstones{0};     // delete markers inca neuitati (S13)
    std::atomic<uint64_t> total_gets{0};
    std::atomic<uint64_t> total_sets{0};
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> keys_evicted{0};
    std::atomic<uint64_t> cold_file_bytes{0};
    std::atomic<uint64_t> cold_obsolete_bytes{0};
    std::atomic<uint64_t> cold_reclaimed_bytes{0};
    std::atomic<uint64_t> aof_bytes{0};
    std::atomic<uint64_t> aof_base_bytes{0};
    std::atomic<uint64_t> aof_synced_bytes{0};     // prefixul durabil al generatiei curente
    std::atomic<uint64_t> aof_pending_bytes{0};    // octeti scrisi inca nesincronizati
    std::atomic<uint64_t> aof_generation{0};
    std::atomic<uint64_t> aof_sync_inflight{0};
    std::atomic<uint64_t> aof_last_sync_epoch_ms{0};
    std::atomic<uint64_t> aof_sync_duration_us{0}; // ultima incercare, inclusiv intarzierea de test
    std::atomic<uint64_t> aof_sync_overruns{0};    // incercari mai lungi decat intervalul tinta
    std::atomic<uint64_t> aof_append_lock_wait_us{0};
    std::atomic<uint64_t> aof_append_lock_waits_slow{0}; // asteptari >= 50ms la mutexul de append
};

extern DbMetrics global_metrics;

// Contor cu un singur scriitor (thread-ul de comenzi: total_gets, total_sets,
// cache_hits, cache_misses, total_commands): load + store e exact si evita
// instructiunea lock a unui fetch_add, platita la fiecare comanda. Cititorii
// (metrici, INFO) vad tot o valoare atomica. Un contor scris si de alt thread
// (keys_in_ram, resident_bytes: watchdog-ul) ramane pe fetch_add.
template <typename T>
inline void bump(std::atomic<T>& counter, const T n = 1) {
    counter.store(counter.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
}

// Varsta ultimei sincronizari reusite; -1 daca nu a existat niciuna.
inline long long aof_sync_age_ms() {
    const auto last = global_metrics.aof_last_sync_epoch_ms.load(std::memory_order_relaxed);
    if (last == 0) return -1;
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto age = now - static_cast<long long>(last);
    return age < 0 ? 0 : age;
}

// Starea peer-ilor de replicare, scrisa de thread-urile replicatorului. phi
// (Hayashibara et al., SRDS 2004) se calculeaza la raport, din istoricul
// heartbeat-urilor: suspiciunea creste cat timp peer-ul tace.
void publish_peer(const std::string& peer, double mean_ms, double stddev_ms, long long last_beat_ms, uint64_t lag_bytes);
std::string peers_report(bool prometheus);

// SEC-8: bind_ip vine din SOMNIUM_METRICS_BIND (implicit 127.0.0.1)
void start_prometheus_exporter(int port, const std::string& bind_ip);
