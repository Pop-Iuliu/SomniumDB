//
// Created by tiwerlol on 08.08.2026.
//

#include "metrics.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <string>
#include <thread>
#include <chrono>
#include <iostream>
#include <cstdio>
#include <cmath>
#include <limits>
#include <map>
#include <mutex>

DbMetrics global_metrics;

namespace {
    struct PeerState {
        double mean_ms = 0;
        double stddev_ms = 0;
        long long last_beat_ms = 0; // 0 = niciun heartbeat inca
        uint64_t lag_bytes = 0;
    };
    std::mutex peers_mutex;
    std::map<std::string, PeerState> peers;

    long long steady_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    // phi = -log10(P(un heartbeat soseste mai tarziu decat acum)), cu intervalele
    // aproximate normal; fara niciun heartbeat, suspiciunea e infinita
    double phi(const PeerState& p) {
        if (p.last_beat_ms == 0) return std::numeric_limits<double>::infinity();
        const double sd = std::max(p.stddev_ms, 100.0); // ca Akka: deviatie minima
        const double later = 0.5 * std::erfc((steady_ms() - p.last_beat_ms - p.mean_ms) / (sd * std::sqrt(2.0)));
        return later < 1e-300 ? 300.0 : -std::log10(later);
    }
} // namespace

void publish_peer(const std::string& peer, const double mean_ms, const double stddev_ms, const long long last_beat_ms,
                  const uint64_t lag_bytes) {
    std::lock_guard lock(peers_mutex);
    peers[peer] = {mean_ms, stddev_ms, last_beat_ms, lag_bytes};
}

std::string peers_report(const bool prometheus) {
    std::lock_guard lock(peers_mutex);
    std::string out;
    for (const auto& [name, p] : peers) {
        const double f = phi(p);
        const std::string value = std::isinf(f) ? (prometheus ? "+Inf" : "inf") : std::to_string(f);
        if (prometheus) {
            out += "db_peer_phi{peer=\"" + name + "\"} " + value + "\n";
            out += "db_peer_lag_bytes{peer=\"" + name + "\"} " + std::to_string(p.lag_bytes) + "\n";
        } else {
            out += "Peer " + name + ": phi=" + value + " lag=" + std::to_string(p.lag_bytes) + "\n";
        }
    }
    return out;
}

static void prometheus_thread(int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "[Metrics] Eroare: Nu s-a putut face bind pe portul " << port << "!\n";
        close(server_fd);
        return;
    }
    if (listen(server_fd, 3) < 0) {
        std::cerr << "[Metrics] Eroare: Nu s-a putut porni listen!\n";
        close(server_fd);
        return;
    }

    std::cout << "[Metrics] Prometheus exporter asculta pe portul " << port << "...\n";

    while (true) {
        int client_socket = accept(server_fd, nullptr, nullptr);
        if (client_socket < 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        char buffer[1024] = {0};
        read(client_socket, buffer, 1024);

        std::string body =
            "# HELP db_keys_in_ram Total keys currently in memory\n"
            "# TYPE db_keys_in_ram gauge\n"
            "db_keys_in_ram " + std::to_string(global_metrics.keys_in_ram.load()) + "\n"
            "# HELP db_resident_bytes Estimated bytes of keys and values resident in RAM\n"
            "# TYPE db_resident_bytes gauge\n"
            "db_resident_bytes " + std::to_string(global_metrics.resident_bytes.load()) + "\n"
            "# HELP db_tombstones Versioned delete markers not yet garbage-collected\n"
            "# TYPE db_tombstones gauge\n"
            "db_tombstones " + std::to_string(global_metrics.tombstones.load()) + "\n"
            "# HELP db_total_gets Total GET commands\n"
            "# TYPE db_total_gets counter\n"
            "db_total_gets " + std::to_string(global_metrics.total_gets.load()) + "\n"
            "# HELP db_total_sets Total SET commands\n"
            "# TYPE db_total_sets counter\n"
            "db_total_sets " + std::to_string(global_metrics.total_sets.load()) + "\n"
            "# HELP db_cache_hits Total successful memory reads\n"
            "# TYPE db_cache_hits counter\n"
            "db_cache_hits " + std::to_string(global_metrics.cache_hits.load()) + "\n"
            "# HELP db_keys_evicted Total keys sent to Cold Storage\n"
            "# TYPE db_keys_evicted counter\n"
            "db_keys_evicted " + std::to_string(global_metrics.keys_evicted.load()) + "\n"
            "# HELP db_cold_file_bytes Current size of the cold storage file\n"
            "# TYPE db_cold_file_bytes gauge\n"
            "db_cold_file_bytes " + std::to_string(global_metrics.cold_file_bytes.load()) + "\n"
            "# HELP db_cold_obsolete_bytes Cold storage bytes no longer referenced by the index\n"
            "# TYPE db_cold_obsolete_bytes gauge\n"
            "db_cold_obsolete_bytes " + std::to_string(global_metrics.cold_obsolete_bytes.load()) + "\n"
            "# HELP db_cold_reclaimed_bytes Total bytes reclaimed by cold storage compaction\n"
            "# TYPE db_cold_reclaimed_bytes counter\n"
            "db_cold_reclaimed_bytes " + std::to_string(global_metrics.cold_reclaimed_bytes.load()) + "\n"
            "# HELP db_aof_bytes Current size of the append-only file\n"
            "# TYPE db_aof_bytes gauge\n"
            "db_aof_bytes " + std::to_string(global_metrics.aof_bytes.load()) + "\n"
            "# HELP db_aof_base_bytes AOF size after the last rewrite (or at startup)\n"
            "# TYPE db_aof_base_bytes gauge\n"
            "db_aof_base_bytes " + std::to_string(global_metrics.aof_base_bytes.load()) + "\n"
            "# HELP db_peer_phi Phi accrual suspicion level per replication peer\n"
            "# TYPE db_peer_phi gauge\n"
            "# HELP db_peer_lag_bytes AOF bytes not yet acknowledged by the peer\n"
            "# TYPE db_peer_lag_bytes gauge\n" + peers_report(true);

        std::string response =
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: " + std::to_string(body.length()) + "\r\n"
            "Connection: close\r\n\r\n" + body;

        write(client_socket, response.c_str(), response.length());
        close(client_socket);
    }
}

void start_prometheus_exporter(int port) {
    std::thread(prometheus_thread, port).detach(); // ruleaza separat de db
}