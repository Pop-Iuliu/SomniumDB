#pragma once

// Parser RESP partajat de retea si de recovery-ul AOF. Lucreaza pe un offset
// (apelantul sterge octetii consumati o singura data pe batch), fara copii
// temporare, si nu aloca pana nu stie ca octetii exista in buffer.

#include <cctype>
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
// Progresul unei comenzi incomplete (left > 0): mai sunt `left` argumente, primul la
// pos + next. Fara el, fiecare citire reparsa comanda de la inceput si ii recopia
// argumentele: un argument mare urmat de octeti trimisi unul cate unul oprea serverul.
struct Partial {
    size_t next = 0;
    uint64_t left = 0;
};

// Cu `partial`, un NeedMore tine minte unde a ramas, iar apelul urmator (acelasi pos,
// acelasi args, buffer doar prelungit) continua de acolo.
inline Status parse(const std::string& buf, const size_t pos, size_t* end, std::vector<std::string>* args,
                    Partial* partial) {
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
    if (partial && partial->left) {
        p = pos + partial->next;
        count = partial->left;
    } else {
        if (const Status st = header('*', kMaxArgs, &count); st != Status::Complete) return st;
        args->clear();
    }

    for (uint64_t i = 0; i < count; ++i) {
        const size_t arg_start = p;
        uint64_t len = 0;
        Status st = header('$', kMaxBulk, &len);
        if (st == Status::Complete) {
            if (buf.size() - p < len + 2) st = Status::NeedMore;
            else if (buf[p + len] != '\r' || buf[p + len + 1] != '\n') st = Status::Malformed;
        }
        if (st != Status::Complete) {
            if (partial) *partial = st == Status::NeedMore ? Partial{arg_start - pos, count - i} : Partial{};
            return st;
        }
        args->emplace_back(buf, p, len);
        p += len + 2;
    }

    if (partial) *partial = {};
    *end = p;
    return Status::Complete;
}

inline Status parse(const std::string& buf, const size_t pos, size_t* end, std::vector<std::string>* args) {
    return parse(buf, pos, end, args, nullptr);
}

constexpr size_t kMaxInline = 64 * 1024; // ca PROTO_INLINE_MAX_SIZE din Redis

// Imparte o linie inline ca sdssplitargs din Redis: argumente separate prin
// spatii; "..." accepta \n \r \t \b \a \xHH, iar orice alt caracter escapat
// ramane literal (\" \\); '...' accepta doar \'. O ghilimea de inchidere
// trebuie urmata de spatiu sau de final. false = ghilimele neechilibrate.
inline bool split_inline(const std::string_view line, std::vector<std::string>* args) {
    const auto space = [](const char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
    const auto hex = [](const char ch) { return std::isxdigit(static_cast<unsigned char>(ch)) != 0; };

    args->clear();
    size_t i = 0;
    while (true) {
        while (i < line.size() && space(line[i])) ++i;
        if (i == line.size()) return true;

        std::string arg;
        char quote = 0;
        for (; i < line.size(); ++i) {
            const char ch = line[i];
            if (!quote) {
                if (space(ch)) break;
                if (ch == '"' || ch == '\'') quote = ch;
                else arg += ch;
            } else if (ch == quote) {
                quote = 0;
                if (i + 1 < line.size() && !space(line[i + 1])) return false;
            } else if (ch == '\\' && i + 1 < line.size()) {
                const char next = line[++i];
                if (quote == '\'') {
                    if (next != '\'') arg += '\\';
                    arg += next;
                } else if (next == 'x' && i + 2 < line.size() && hex(line[i + 1]) && hex(line[i + 2])) {
                    unsigned char byte = 0;
                    std::from_chars(line.data() + i + 1, line.data() + i + 3, byte, 16);
                    arg += static_cast<char>(byte);
                    i += 2;
                } else {
                    constexpr std::string_view from = "nrtba", to = "\n\r\t\b\a";
                    const size_t k = from.find(next);
                    arg += k == std::string_view::npos ? next : to[k];
                }
            } else {
                arg += ch;
            }
        }
        if (quote) return false;
        args->push_back(std::move(arg));
    }
}

// Cererile clientilor: array RESP sau comanda inline (telnet, redis-benchmark
// PING_INLINE), o linie terminata cu LF sau CRLF. AOF-ul ramane strict.
inline Status parse_request(const std::string& buf, const size_t pos, size_t* end, std::vector<std::string>* args,
                            Partial* partial) {
    if (pos >= buf.size() || buf[pos] == '*') return parse(buf, pos, end, args, partial);

    const std::string_view window = std::string_view(buf).substr(pos, kMaxInline);
    const size_t nl = window.find('\n');
    if (nl == std::string_view::npos) {
        return window.size() < kMaxInline ? Status::NeedMore : Status::Malformed;
    }

    std::string_view line = window.substr(0, nl);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

    if (!split_inline(line, args)) return Status::Malformed;
    *end = pos + nl + 1; // o linie goala e o comanda goala: ignorata
    return Status::Complete;
}

inline Status parse_request(const std::string& buf, const size_t pos, size_t* end, std::vector<std::string>* args) {
    return parse_request(buf, pos, end, args, nullptr);
}

} // namespace resp
