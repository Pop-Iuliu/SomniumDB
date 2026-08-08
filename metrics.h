//
// Created by tiwerlol on 08.08.2026.
//

#ifndef REDIS_METRICS_H
#define REDIS_METRICS_H

#ifndef METRICS_H
#define METRICS_H

#include <atomic>
#include <cstdint>

struct DbMetrics {
    std::atomic<uint64_t> keys_in_ram{0};
    std::atomic<uint64_t> total_gets{0};
    std::atomic<uint64_t> total_sets{0};
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> bloom_prevented_disk_reads{0};
    std::atomic<uint64_t> keys_evicted{0};
};

extern DbMetrics global_metrics;

void start_prometheus_exporter(int port = 9090);

#endif

#endif //REDIS_METRICS_H
