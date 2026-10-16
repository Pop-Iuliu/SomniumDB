#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// REF-2: toata configurarea vine din mediu si e citita o singura data, de
// load_config(). Valorile implicite sunt doar aici; fiecare componenta primeste
// Config-ul la constructie si isi copiaza ce ii trebuie.

enum class AofSync { Always, Everysec, Off }; // SOMNIUM_AOF_SYNC: always / everysec / no

struct Config {
    // serverul (main.cpp)
    int port = 6379;                              // REDIS_PORT
    std::string bind = "127.0.0.1";               // SOMNIUM_BIND (IPv4); SEC-1: implicit doar localhost
    std::string password;                         // SOMNIUM_PASSWORD; gol = fara autentificare
    uint64_t maxclients = 10000;                  // SOMNIUM_MAXCLIENTS
    uint64_t max_client_buffers = 1ull << 30;     // SOMNIUM_MAX_CLIENT_BUFFERS
    std::optional<std::string> tls_cert, tls_key; // SOMNIUM_TLS_CERT, SOMNIUM_TLS_KEY (setata, chiar goala, conteaza)
    bool metrics = true;                          // false daca SOMNIUM_NO_METRICS e setata
    std::string metrics_bind = "127.0.0.1";       // SOMNIUM_METRICS_BIND
    int trace_fd = -1;                            // SOMNIUM_TRACE_FD (depanare): -1 oprit, -2 tot

    // replicarea
    uint32_t node_id = 1;                                    // SOMNIUM_NODE_ID
    std::vector<std::pair<std::string, std::string>> peers; // SOMNIUM_PEERS: (host, port)

    // baza de date
    uint64_t max_rooms = 10000;             // SOMNIUM_MAX_ROOMS
    long long max_clock_offset_ms = 600000; // SOMNIUM_MAX_CLOCK_OFFSET_MS
    long long clock_offset_ms = 0;          // SOMNIUM_CLOCK_OFFSET_MS (teste)
    uint64_t max_keys = 1000000;            // SOMNIUM_MAX_KEYS, pe camera
    uint64_t maxmemory = 0;                 // SOMNIUM_MAXMEMORY; 0 = fara limita

    // persistenta
    AofSync aof_sync = AofSync::Everysec;         // SOMNIUM_AOF_SYNC
    uint64_t aof_rewrite_min_bytes = 64ull << 20; // SOMNIUM_AOF_REWRITE_MIN_BYTES
    long long aof_sync_interval_ms = 1000;        // SOMNIUM_AOF_SYNC_INTERVAL_MS, 1..60000
    int aof_sync_delay_ms = 0;                    // SOMNIUM_AOF_SYNC_DELAY_MS (teste), 0..60000
    int snapshot_delay_ms = 0;                    // SOMNIUM_SNAPSHOT_DELAY_MS (teste), 0..60000
};

// nullopt = configuratie cu care serverul refuza sa porneasca (mesajul e deja
// afisat). O valoare invalida a celorlalte variabile e raportata si ramane implicita.
std::optional<Config> load_config();
