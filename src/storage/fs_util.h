#pragma once

// Utilitare I/O partajate de AOF si snapshot-uri: scrieri complete,
// sincronizare pe disc si parsare numerica stricta (AOF-ul e date
// nesigur de la disk, nu input de la client de incredere).

#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <climits>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace fsutil {

// scriere completa (write poate pleca partial sau cu EINTR)
inline bool write_all(int fd, const char* data, size_t len) {
    size_t off = 0;
    while (off < len) {
        const ssize_t n = ::write(fd, data + off, len - off);
        if (n > 0) {
            off += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

// fdatasync: pentru append-uri e suficient (dimensiunea modificata e acoperita)
inline bool sync_fd(int fd) {
    return ::fdatasync(fd) == 0;
}

// fsync pe director: ca redenumirile/crearea fisierelor sa supravietuiasca
// si unei piete de curent, nu doar unui crash de proces
inline void sync_dir() {
    const int d = ::open(".", O_RDONLY | O_DIRECTORY);
    if (d < 0) return;
    ::fsync(d);
    ::close(d);
}

// parsare stricta fara semn: doar cifre, fara spatii, semne sau overflow
inline bool parse_u64(const std::string& s, uint64_t* out) {
    if (s.empty()) return false;
    uint64_t v = 0;
    for (const char c : s) {
        if (c < '0' || c > '9') return false;
        const uint64_t d = static_cast<uint64_t>(c - '0');
        if (v > (UINT64_MAX - d) / 10) return false;
        v = v * 10 + d;
    }
    *out = v;
    return true;
}

// parsare stricta cu semn, cu limita pe ambele capete
inline bool parse_i64(const std::string& s, long long* out) {
    const bool neg = !s.empty() && s[0] == '-';
    uint64_t u = 0;
    if (!parse_u64(neg ? s.substr(1) : s, &u)) return false;
    if (!neg) {
        if (u > static_cast<uint64_t>(LLONG_MAX)) return false;
        *out = static_cast<long long>(u);
    } else {
        if (u > static_cast<uint64_t>(LLONG_MAX) + 1) return false;
        *out = (u == static_cast<uint64_t>(LLONG_MAX) + 1) ? LLONG_MIN : -static_cast<long long>(u);
    }
    return true;
}

} // namespace fsutil
