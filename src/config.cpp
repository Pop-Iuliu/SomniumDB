#include "config.h"
#include "storage/fs_util.h"
#include <algorithm>
#include <arpa/inet.h>
#include <cstdio>
#include <cstdlib>
#include <sys/socket.h>

namespace {
    // valoare invalida: raportata, iar variabila ramane la valoarea implicita
    void invalid(const char* name, const char* value, const std::string& note = "") {
        fprintf(stderr, "%s invalid: '%s'%s\n", name, value, note.c_str());
    }
} // namespace

std::optional<Config> load_config() {
    Config cfg;

    // fara acestea serverul nu porneste
    if (const char* v = getenv("REDIS_PORT")) {
        try {
            cfg.port = std::stoi(v);
        } catch (...) {
            fprintf(stderr, "REDIS_PORT invalid: %s\n", v);
            return std::nullopt;
        }
    }
    if (const char* v = getenv("SOMNIUM_MAXCLIENTS"); v && (!fsutil::parse_u64(v, &cfg.maxclients) || cfg.maxclients == 0)) {
        fprintf(stderr, "SOMNIUM_MAXCLIENTS invalid: %s\n", v);
        return std::nullopt;
    }
    if (const char* v = getenv("SOMNIUM_MAX_CLIENT_BUFFERS"); v && !fsutil::parse_u64(v, &cfg.max_client_buffers)) {
        fprintf(stderr, "SOMNIUM_MAX_CLIENT_BUFFERS invalid: %s\n", v);
        return std::nullopt;
    }
    if (const char* v = getenv("SOMNIUM_BIND")) cfg.bind = v;
    in_addr addr{};
    if (inet_pton(AF_INET, cfg.bind.c_str(), &addr) != 1) {
        fprintf(stderr, "SOMNIUM_BIND invalid: %s (astept o adresa IPv4)\n", cfg.bind.c_str());
        return std::nullopt;
    }
    if (const char* v = getenv("SOMNIUM_PASSWORD")) cfg.password = v;
    if (ntohl(addr.s_addr) >> 24 != 127 && cfg.password.empty()) { // SEC-2: in retea, doar cu parola
        fprintf(stderr, "Refuz sa pornesc: SOMNIUM_BIND=%s expune serverul in retea, dar SOMNIUM_PASSWORD nu e setat\n",
                cfg.bind.c_str());
        return std::nullopt;
    }

    // fisierele TLS se valideaza la incarcare (main.cpp); SOMNIUM_METRICS_BIND, la bind-ul exporterului
    if (const char* v = getenv("SOMNIUM_TLS_CERT")) cfg.tls_cert = v;
    if (const char* v = getenv("SOMNIUM_TLS_KEY")) cfg.tls_key = v;
    cfg.metrics = getenv("SOMNIUM_NO_METRICS") == nullptr;
    if (const char* v = getenv("SOMNIUM_METRICS_BIND")) cfg.metrics_bind = v;
    if (const char* v = getenv("SOMNIUM_TRACE_FD")) cfg.trace_fd = atoi(v);

    // de aici incolo, o valoare invalida e raportata si ramane cea implicita
    if (const char* v = getenv("SOMNIUM_NODE_ID")) {
        uint64_t id = 0;
        if (fsutil::parse_u64(v, &id) && id >= 1 && id <= UINT32_MAX) cfg.node_id = static_cast<uint32_t>(id);
        else invalid("SOMNIUM_NODE_ID", v, " (folosesc 1)");
    }
    // o singura lista pentru replicator si pentru colectarea tombstone-urilor (S13)
    const char* list = getenv("SOMNIUM_PEERS");
    const std::string peers = list ? list : "";
    for (size_t start = 0; start < peers.size();) {
        const size_t comma = std::min(peers.find(',', start), peers.size());
        const std::string item = peers.substr(start, comma - start);
        start = comma + 1;
        const size_t colon = item.rfind(':');
        if (colon == std::string::npos || colon == 0 || colon + 1 == item.size()) {
            fprintf(stderr, "SOMNIUM_PEERS: intrare invalida '%s' (astept host:port)\n", item.c_str());
            continue;
        }
        cfg.peers.emplace_back(item.substr(0, colon), item.substr(colon + 1));
    }

    if (const char* v = getenv("SOMNIUM_MAX_ROOMS"); v && !fsutil::parse_u64(v, &cfg.max_rooms)) {
        invalid("SOMNIUM_MAX_ROOMS", v, " (folosesc " + std::to_string(cfg.max_rooms) + ")");
    }
    if (const char* v = getenv("SOMNIUM_MAX_CLOCK_OFFSET_MS"); v && !fsutil::parse_i64(v, &cfg.max_clock_offset_ms)) {
        invalid("SOMNIUM_MAX_CLOCK_OFFSET_MS", v);
    }
    if (const char* v = getenv("SOMNIUM_CLOCK_OFFSET_MS")) (void)fsutil::parse_i64(v, &cfg.clock_offset_ms); // invalid: 0
    if (const char* v = getenv("SOMNIUM_MAX_KEYS"); v && !fsutil::parse_u64(v, &cfg.max_keys)) {
        invalid("SOMNIUM_MAX_KEYS", v, " (folosesc " + std::to_string(cfg.max_keys) + ")");
    }
    if (const char* v = getenv("SOMNIUM_MAXMEMORY"); v && !fsutil::parse_u64(v, &cfg.maxmemory)) {
        invalid("SOMNIUM_MAXMEMORY", v, " (fara limita)");
    }

    if (const char* v = getenv("SOMNIUM_AOF_SYNC")) {
        const std::string sync = v;
        if (sync == "always") cfg.aof_sync = AofSync::Always;
        else if (sync == "no" || sync == "none" || sync == "off") cfg.aof_sync = AofSync::Off;
        else if (sync != "everysec") fprintf(stderr, "SOMNIUM_AOF_SYNC necunoscut: '%s' (folosesc everysec)\n", v);
    }
    if (const char* v = getenv("SOMNIUM_AOF_REWRITE_MIN_BYTES"); v && !fsutil::parse_u64(v, &cfg.aof_rewrite_min_bytes)) {
        invalid("SOMNIUM_AOF_REWRITE_MIN_BYTES", v);
    }
    if (const char* v = getenv("SOMNIUM_AOF_SYNC_INTERVAL_MS"); v && !fsutil::parse_i64(v, &cfg.aof_sync_interval_ms)) {
        invalid("SOMNIUM_AOF_SYNC_INTERVAL_MS", v, " (folosesc 1000)");
    }
    cfg.aof_sync_interval_ms = std::clamp(cfg.aof_sync_interval_ms, 1LL, 60000LL);

    // intarzieri de test
    long long delay = 0;
    if (const char* v = getenv("SOMNIUM_AOF_SYNC_DELAY_MS"); v && !fsutil::parse_i64(v, &delay)) {
        invalid("SOMNIUM_AOF_SYNC_DELAY_MS", v);
    }
    cfg.aof_sync_delay_ms = delay >= 0 && delay <= 60000 ? static_cast<int>(delay) : 0;
    if (const char* v = getenv("SOMNIUM_SNAPSHOT_DELAY_MS"); v && v[0]) {
        long long n = 0;
        if (fsutil::parse_i64(v, &n) && n >= 0 && n <= 60000) cfg.snapshot_delay_ms = static_cast<int>(n);
        else invalid("SOMNIUM_SNAPSHOT_DELAY_MS", v);
    }
    return cfg;
}
