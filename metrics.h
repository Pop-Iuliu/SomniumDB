//
// Created by tiwerlol on 08.08.2026.
//

#pragma once

#include <atomic>
#include <cstdint>

struct DbMetrics {
    std::atomic<uint64_t> keys_in_ram{0};
    std::atomic<uint64_t> resident_bytes{0}; // estimare, vezi record_cost()
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
};

extern DbMetrics global_metrics;

void start_prometheus_exporter(int port = 9090);
