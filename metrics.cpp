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

DbMetrics global_metrics;

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
            "db_aof_base_bytes " + std::to_string(global_metrics.aof_base_bytes.load()) + "\n";

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