#pragma once

// Parser RESP partajat de retea si de recovery-ul AOF. Lucreaza pe un offset
// (apelantul sterge octetii consumati o singura data pe batch), fara copii
// temporare, si nu aloca pana nu stie ca octetii exista in buffer.

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace resp {

enum class Status { Complete, NeedMore, Malformed };

constexpr uint64_t kMaxArgs = 1024 * 1024;
constexpr uint64_t kMaxBulk = 512ull * 1024 * 1024; // ca proto-max-bulk-len din Redis
constexpr size_t kMaxHeader = 32;                   // prefix + 20 cifre + CRLF incap lejer

// Un array "*N\r\n" urmat de N bulk-uri "$len\r\n<octeti>\r\n", citit de la pos.
// Complete: *end trece de comanda si *args o contine. NeedMore: un prefix
// valid, asteptam octeti. Malformed: nicio continuare nu il mai face valid.
inline Status parse(const std::string& buf, const size_t pos, size_t* end, std::vector<std::string>* args) {
    size_t p = pos;

    // "<prefix><numar zecimal fara semn>\r\n", cu cautarea CRLF marginita
    const auto header = [&](const char prefix, const uint64_t max, uint64_t* out) {
        if (p >= buf.size()) return Status::NeedMore;
        if (buf[p] != prefix) return Status::Malformed;
        const std::string_view window = std::string_view(buf).substr(p, kMaxHeader);
        const size_t crlf = window.find("\r\n");
        if (crlf == std::string_view::npos) {
            return window.size() < kMaxHeader ? Status::NeedMore : Status::Malformed;
        }
        const char* digits_end = window.data() + crlf;
        const auto [parsed, ec] = std::from_chars(window.data() + 1, digits_end, *out);
        if (ec != std::errc() || parsed != digits_end || *out > max) return Status::Malformed;
        p += crlf + 2;
        return Status::Complete;
    };

    uint64_t count = 0;
    if (const Status st = header('*', kMaxArgs, &count); st != Status::Complete) return st;

    args->clear();
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t len = 0;
        if (const Status st = header('$', kMaxBulk, &len); st != Status::Complete) return st;
        if (buf.size() - p < len + 2) return Status::NeedMore;
        if (buf[p + len] != '\r' || buf[p + len + 1] != '\n') return Status::Malformed;
        args->emplace_back(buf, p, len);
        p += len + 2;
    }

    *end = p;
    return Status::Complete;
}

constexpr size_t kMaxInline = 64 * 1024; // ca PROTO_INLINE_MAX_SIZE din Redis

// Cererile clientilor: array RESP sau comanda inline (telnet, redis-benchmark
// PING_INLINE), o linie terminata cu LF sau CRLF, impartita pe spatii si
// taburi. Fara ghilimele: clientii reali trimit array-uri. AOF-ul ramane strict.
inline Status parse_request(const std::string& buf, const size_t pos, size_t* end, std::vector<std::string>* args) {
    if (pos >= buf.size() || buf[pos] == '*') return parse(buf, pos, end, args);

    const std::string_view window = std::string_view(buf).substr(pos, kMaxInline);
    const size_t nl = window.find('\n');
    if (nl == std::string_view::npos) {
        return window.size() < kMaxInline ? Status::NeedMore : Status::Malformed;
    }

    std::string_view line = window.substr(0, nl);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

    args->clear();
    for (size_t i = line.find_first_not_of(" \t"); i != std::string_view::npos;
         i = line.find_first_not_of(" \t", i)) {
        const size_t stop = std::min(line.find_first_of(" \t", i), line.size());
        args->emplace_back(line.substr(i, stop - i));
        i = stop;
    }
    *end = pos + nl + 1; // o linie goala e o comanda goala: ignorata
    return Status::Complete;
}

} // namespace resp
