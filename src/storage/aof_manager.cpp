#include "aof_manager.h"
#include "fs_util.h"
#include <cstdio>
#include <cerrno>
#include <cstdlib>
#include <chrono>
#include <cstring>
#include <fstream>
#include <climits>
#include <fcntl.h>
#include <sys/stat.h>

namespace {
    constexpr char kAofPath[] = "appendonly.aof";
    constexpr char kAofHeader[] = "*2\r\n$11\r\nSOMNIUM-AOF\r\n$1\r\n3\r\n";
    constexpr char kMagicToken[] = "SOMNIUM-AOF";
    constexpr char kMetaToken[] = "SOMNIUM-META";
    constexpr int kAofVersion = 3;

    // comenzi de scriere: doar ele apar in AOF (in ambele formate vechi)
    bool is_write_cmd(const std::string& c) {
        return c == "SET" || c == "DEL" || c == "CRDTMERGE";
    }

    enum class Fmt { Undetected, V3, V2, V1 };

    // din tokens in structura de mutatie, in functie de format
    bool build_record(const Fmt fmt, const std::vector<std::string>& tokens, AofRecord* rec) {
        if (fmt == Fmt::V3) {
            // [room, CMD, args..., META, expire, ts, node]
            if (tokens.size() < 5) return false;
            const size_t n = tokens.size();
            if (tokens[n - 4] != kMetaToken) return false;
            if (!fsutil::parse_i64(tokens[n - 3], &rec->expire_at)) return false;
            if (!fsutil::parse_u64(tokens[n - 2], &rec->timestamp_ms)) return false;
            uint64_t node = 0;
            if (!fsutil::parse_u64(tokens[n - 1], &node) || node > UINT32_MAX) return false;
            rec->node_id = static_cast<uint32_t>(node);
            rec->room = tokens[0];
            rec->args.assign(tokens.begin() + 1, tokens.end() - 4);
        } else if (fmt == Fmt::V2) {
            if (tokens.empty()) return false;
            rec->room = tokens[0];
            rec->args.assign(tokens.begin() + 1, tokens.end());
            // v2 nu serializa meta: expirarea si versiunea CRDT sunt necunoscute
            rec->expire_at = 0;
            rec->timestamp_ms = 0;
            rec->node_id = 1;
        } else { // V1
            rec->room = "default";
            rec->args = tokens;
            rec->expire_at = 0;
            rec->timestamp_ms = 0;
            rec->node_id = 1;
        }
        return true;
    }
} // namespace

AOFManager::AOFManager() {
    const char* env = getenv("SOMNIUM_AOF_SYNC");
    const std::string val = env ? env : "everysec";
    if (val == "always") sync_policy_ = SyncPolicy::Always;
    else if (val == "everysec") sync_policy_ = SyncPolicy::Everysec;
    else if (val == "no" || val == "none" || val == "off") sync_policy_ = SyncPolicy::Off;
    else {
        sync_policy_ = SyncPolicy::Everysec;
        fprintf(stderr, "SOMNIUM_AOF_SYNC necunoscut: '%s' (folosesc everysec)\n", val.c_str());
    }
}

AOFManager::~AOFManager() {
    std::lock_guard lock(mutex_);
    if (fd_ >= 0) {
        if (sync_policy_ != SyncPolicy::Off && dirty_.load(std::memory_order_relaxed)) {
            fsutil::sync_fd(fd_);
        }
        ::close(fd_);
        fd_ = -1;
    }
    if (rewrite_fd_ >= 0) {
        ::close(rewrite_fd_);
        rewrite_fd_ = -1;
        ::unlink("appendonly.aof.tmp");
    }
}

const char* AOFManager::policy_name() const {
    switch (sync_policy_) {
        case SyncPolicy::Always: return "always";
        case SyncPolicy::Everysec: return "everysec";
        default: return "no";
    }
}

long long AOFManager::now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string AOFManager::encode_record(const AofRecord& rec) {
    // room + comanda + argumente + 4 tokenuri de meta
    std::string out = "*" + std::to_string(rec.args.size() + 5) + "\r\n";
    const auto bulk = [&out](const std::string& v) {
        out += '$';
        out += std::to_string(v.size());
        out += "\r\n";
        out += v;
        out += "\r\n";
    };
    bulk(rec.room);
    for (const auto& a : rec.args) bulk(a);
    bulk(kMetaToken);
    bulk(std::to_string(rec.expire_at));
    bulk(std::to_string(rec.timestamp_ms));
    bulk(std::to_string(rec.node_id));
    return out;
}

AOFManager::ParseStatus AOFManager::parse_resp_array(const std::string& buf, const size_t pos,
                                                     size_t* rec_end, std::vector<std::string>* tokens) {
    size_t p = pos;
    if (p >= buf.size()) return ParseStatus::NeedMore;
    if (buf[p] != '*') return ParseStatus::Malformed;

    const size_t hdr = buf.find("\r\n", p);
    if (hdr == std::string::npos) return ParseStatus::NeedMore;

    uint64_t count = 0;
    if (!fsutil::parse_u64(buf.substr(p + 1, hdr - p - 1), &count) || count == 0 || count > 1000000) {
        return ParseStatus::Malformed;
    }
    p = hdr + 2;

    tokens->clear();
    tokens->reserve(static_cast<size_t>(count));

    for (uint64_t i = 0; i < count; ++i) {
        if (p >= buf.size()) return ParseStatus::NeedMore;
        if (buf[p] != '$') return ParseStatus::Malformed;

        const size_t len_end = buf.find("\r\n", p);
        if (len_end == std::string::npos) return ParseStatus::NeedMore;

        uint64_t len = 0;
        if (!fsutil::parse_u64(buf.substr(p + 1, len_end - p - 1), &len) || len > (1ull << 32)) {
            return ParseStatus::Malformed;
        }
        p = len_end + 2;
        // niciodata alocam inainte sa stim ca bytes exista in buffer
        if (p + len + 2 > buf.size()) return ParseStatus::NeedMore;

        tokens->emplace_back(buf, p, static_cast<size_t>(len));
        p += len + 2;
    }

    *rec_end = p;
    return ParseStatus::Complete;
}

bool AOFManager::sync_locked(const int fd) {
    if (!fsutil::sync_fd(fd)) {
        healthy_ = false;
        fprintf(stderr, "AOF: fdatasync a esuat: %s (persistenta suspecta!)\n", strerror(errno));
        return false;
    }
    last_sync_ms_.store(now_ms(), std::memory_order_relaxed);
    return true;
}

// presupune mutex_ deja prins (chemata din watchdog si din append la always)
void AOFManager::sync_due_locked() {
    if (sync_policy_ != SyncPolicy::Everysec) return;
    if (fd_ < 0 || !dirty_.load(std::memory_order_relaxed)) return;
    if (now_ms() - last_sync_ms_.load(std::memory_order_relaxed) < 1000) return;

    if (sync_locked(fd_)) dirty_.store(false, std::memory_order_relaxed);
}

void AOFManager::sync_if_due() {
    if (sync_policy_ != SyncPolicy::Everysec) return;
    if (!dirty_.load(std::memory_order_relaxed)) return;

    std::lock_guard lock(mutex_);
    sync_due_locked();
}

bool AOFManager::open_file() {
    std::lock_guard lock(mutex_);
    if (fd_ >= 0) return true;

    fd_ = ::open(kAofPath, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        healthy_ = false;
        fprintf(stderr, "AOF: nu pot deschide %s: %s\n", kAofPath, strerror(errno));
        return false;
    }

    // fisier nou: header de format, ca deschiderile viitoare sa nu mai
    // ghiceasca nimic din continut
    struct stat sb{};
    if (::fstat(fd_, &sb) == 0 && sb.st_size == 0) {
        if (!fsutil::write_all(fd_, kAofHeader, sizeof(kAofHeader) - 1) || !sync_locked(fd_)) {
            fprintf(stderr, "AOF: nu pot scrie headerul v3\n");
            healthy_ = false;
            ::close(fd_);
            fd_ = -1;
            return false;
        }
        fsutil::sync_dir();
    }

    printf("AOF: format v3, politica durabilitate: %s\n", policy_name());
    return true;
}

bool AOFManager::append(const std::string& room_name, const std::vector<std::string>& args,
                        const long long expire_at, const uint64_t timestamp_ms, const uint32_t node_id) {
    std::lock_guard lock(mutex_);
    if (replaying_) return false;

    AofRecord rec;
    rec.room = room_name;
    rec.args = args;
    rec.expire_at = expire_at;
    rec.timestamp_ms = timestamp_ms;
    rec.node_id = node_id;
    const std::string data = encode_record(rec);

    if (fd_ < 0) {
        fd_ = ::open(kAofPath, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            healthy_ = false;
            return false;
        }
        healthy_ = true;
    }

    if (!fsutil::write_all(fd_, data.data(), data.size())) {
        healthy_ = false;
        fprintf(stderr, "AOF: scriere esuata: %s (mutatia exista doar in RAM!)\n", strerror(errno));
        ::close(fd_);
        fd_ = -1;
        return false;
    }

    switch (sync_policy_) {
        case SyncPolicy::Always:
            if (!sync_locked(fd_)) return false;
            break;
        case SyncPolicy::Everysec:
            // sincronizarea in "cel mult o secunda" o face watchdog-ul, nu
            // thread-ul de comenzi: un fdatasync lent (disc ocupat) nu are
            // voie sa ingheate clientii
            dirty_.store(true, std::memory_order_relaxed);
            break;
        default:
            dirty_.store(true, std::memory_order_relaxed);
            break;
    }
    return true;
}

void AOFManager::recover(const std::function<void(const AofRecord&)>& replay) {
    struct stat sb{};
    if (::stat(kAofPath, &sb) != 0 || sb.st_size == 0) {
        replaying_ = false;
        return;
    }

    std::ifstream file(kAofPath, std::ios::binary);
    if (!file.is_open()) {
        replaying_ = false;
        return;
    }

    auto preserve_as_corrupt = [&](const char* reason) {
        const std::string dst = std::string(kAofPath) + ".corrupt." + std::to_string(now_ms());
        if (::rename(kAofPath, dst.c_str()) != 0) {
            fprintf(stderr, "AOF: corupt (%s) si redenumirea de diagnostic a esuat: %s\n", reason, strerror(errno));
        } else {
            printf("AOF: fisier corupt (%s); pastrat pentru diagnostic: %s\n", reason, dst.c_str());
        }
        // starea redata pana aici (posibil partiala) trebuie reserializata in
        // v3, ca sa nu se piarda la urmatorul restart
        needs_rewrite_ = true;
    };

    auto truncate_tail = [&](const size_t valid_bytes) {
        const int fd = ::open(kAofPath, O_WRONLY);
        if (fd < 0) {
            fprintf(stderr, "AOF: nu pot trunchia coada incompleta: %s\n", strerror(errno));
            return;
        }
        if (::ftruncate(fd, static_cast<off_t>(valid_bytes)) != 0) {
            fprintf(stderr, "AOF: trunchiere esuata: %s\n", strerror(errno));
        } else {
            fsutil::sync_fd(fd);
            printf("AOF: coada incompleta (crash la scriere) trunchiata la offset %zu\n", valid_bytes);
        }
        ::close(fd);
    };

    Fmt fmt = Fmt::Undetected;
    std::string buf;
    size_t pos = 0;        // offset in buf (pos avanseaza cu fiecare inregistrare)
    size_t compacted = 0;  // octeti deja consumati si dati la o parte din buf
    size_t replayed = 0;

    // citire incrementală: completa bufferul din fisier; la nevoie compacta
    // partea deja consumata ca bufferul sa nu creasca nelimitat
    auto fill = [&]() -> bool {
        if (pos > 0) {
            buf.erase(0, pos);
            compacted += pos;
            pos = 0;
        }
        char chunk[1 << 16];
        file.read(chunk, sizeof(chunk));
        const size_t got = static_cast<size_t>(file.gcount());
        if (got == 0) return false;
        buf.append(chunk, got);
        return true;
    };

    while (true) {
        std::vector<std::string> tokens;
        size_t rec_end = 0;
        ParseStatus st;
        while ((st = parse_resp_array(buf, pos, &rec_end, &tokens)) == ParseStatus::NeedMore) {
            if (!fill()) break; // EOF in mijlocul unei inregistrari
        }

        if (st == ParseStatus::NeedMore) {
            if (pos < buf.size()) truncate_tail(compacted + pos);
            break; // istoricul complet ramane neatins
        }
        if (st == ParseStatus::Malformed) {
            preserve_as_corrupt("inregistrare malformata");
            break;
        }

        const size_t rec_len = rec_end - pos;
        pos = rec_end;

        if (fmt == Fmt::Undetected) {
            if (tokens.size() == 2 && tokens[0] == kMagicToken) {
                uint64_t ver = 0;
                if (!fsutil::parse_u64(tokens[1], &ver)) {
                    preserve_as_corrupt("header ilegibil");
                    break;
                }
                if (ver != kAofVersion) {
                    preserve_as_corrupt("versiune viitoare, nesuportata");
                    break;
                }
                fmt = Fmt::V3;
                continue; // headerul se consuma, nu se redate
            }
            // fara header: formatele vechi. Distinctia v1/v2 se stabileste o
            // singura data, pe prima inregistrare (nu per comanda, ca inainte):
            // in v2, comanda e al doilea token; in v1, primul.
            if (tokens.size() >= 2 && is_write_cmd(tokens[1])) fmt = Fmt::V2;
            else fmt = Fmt::V1;
            needs_rewrite_ = true;
        }

        AofRecord rec;
        if (!build_record(fmt, tokens, &rec)) {
            preserve_as_corrupt("meta invalida sau lipsa");
            break;
        }
        replay(rec);
        replayed++;
    }

    file.close();
    printf("AOF Recovery: %zu inregistrari redate (format: %s)\n", replayed,
           fmt == Fmt::V3 ? "v3" : fmt == Fmt::V2 ? "v2" : fmt == Fmt::V1 ? "v1" : "necunoscut");
    replaying_ = false;
}

bool AOFManager::start_rewrite() {
    std::lock_guard lock(mutex_);
    if (rewrite_fd_ >= 0) return true;
    rewrite_fd_ = ::open("appendonly.aof.tmp", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (rewrite_fd_ < 0) {
        fprintf(stderr, "AOF: migrare v3: nu pot crea fisierul temporar: %s\n", strerror(errno));
        return false;
    }
    if (!fsutil::write_all(rewrite_fd_, kAofHeader, sizeof(kAofHeader) - 1)) {
        fprintf(stderr, "AOF: migrare v3: headerul nu a putut fi scris\n");
        ::close(rewrite_fd_);
        rewrite_fd_ = -1;
        ::unlink("appendonly.aof.tmp");
        return false;
    }
    return true;
}

bool AOFManager::append_rewrite(const AofRecord& rec) {
    std::lock_guard lock(mutex_);
    if (rewrite_fd_ < 0) return false;
    const std::string data = encode_record(rec);
    if (!fsutil::write_all(rewrite_fd_, data.data(), data.size())) {
        fprintf(stderr, "AOF: migrare v3: scriere esuata: %s\n", strerror(errno));
        return false;
    }
    return true;
}

void AOFManager::abort_locked() {
    if (rewrite_fd_ >= 0) {
        ::close(rewrite_fd_);
        rewrite_fd_ = -1;
    }
    ::unlink("appendonly.aof.tmp");
    // fisierul original (vechi) ramane neatins: migrarea poate fi reincercata
}

void AOFManager::abort_rewrite() {
    std::lock_guard lock(mutex_);
    abort_locked();
}

bool AOFManager::commit_rewrite() {
    std::lock_guard lock(mutex_);
    if (rewrite_fd_ < 0) return false;

    if (!fsutil::sync_fd(rewrite_fd_)) {
        fprintf(stderr, "AOF: migrare v3: fdatasync esuat: %s\n", strerror(errno));
        abort_locked();
        return false;
    }
    if (::close(rewrite_fd_) != 0) {
        healthy_ = false;
        fprintf(stderr, "AOF: migrare v3: close esuat: %s\n", strerror(errno));
        rewrite_fd_ = -1;
        ::unlink("appendonly.aof.tmp");
        return false;
    }
    rewrite_fd_ = -1;

    // substitutie atomica: ori vechiul fisier, ori cel nou v3, niciodata unul partial
    if (::rename("appendonly.aof.tmp", kAofPath) != 0) {
        healthy_ = false;
        fprintf(stderr, "AOF: migrare v3: rename esuat: %s\n", strerror(errno));
        ::unlink("appendonly.aof.tmp");
        return false;
    }
    fsutil::sync_dir();

    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    fd_ = ::open(kAofPath, O_WRONLY | O_CREAT | O_APPEND, 0644);
    healthy_ = fd_ >= 0;
    needs_rewrite_ = false;
    printf("AOF: migrare catre formatul v3 finalizata\n");
    return healthy_;
}
