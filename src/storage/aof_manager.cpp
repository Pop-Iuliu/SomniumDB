#include "aof_manager.h"
#include "fs_util.h"
#include "../core/resp.h"
#include "../../metrics.h"
#include <algorithm>
#include <cstdio>
#include <cerrno>
#include <cstdlib>
#include <chrono>
#include <cstring>
#include <fstream>
#include <climits>
#include <thread>
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
    if (const char* min = getenv("SOMNIUM_AOF_REWRITE_MIN_BYTES"); min && !fsutil::parse_u64(min, &rewrite_min_)) {
        fprintf(stderr, "SOMNIUM_AOF_REWRITE_MIN_BYTES invalid: '%s'\n", min);
    }
    long long interval = 1000;
    if (const char* env = getenv("SOMNIUM_AOF_SYNC_INTERVAL_MS"); env && !fsutil::parse_i64(env, &interval)) {
        fprintf(stderr, "SOMNIUM_AOF_SYNC_INTERVAL_MS invalid: '%s' (folosesc 1000)\n", env);
        interval = 1000;
    }
    if (interval < 1) interval = 1;
    if (interval > 60000) interval = 60000;
    sync_interval_ms_ = interval;
    long long delay = 0;
    if (const char* env = getenv("SOMNIUM_AOF_SYNC_DELAY_MS"); env && !fsutil::parse_i64(env, &delay)) {
        fprintf(stderr, "SOMNIUM_AOF_SYNC_DELAY_MS invalid: '%s'\n", env);
        delay = 0;
    }
    if (delay < 0 || delay > 60000) delay = 0;
    sync_delay_ms_ = static_cast<int>(delay);
    next_attempt_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(sync_interval_ms_);
    if (sync_policy_ == SyncPolicy::Everysec) {
        sync_thread_ = std::thread([this] { sync_worker(); });
    }
}

void AOFManager::publish_sizes() const {
    global_metrics.aof_bytes.store(size_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    global_metrics.aof_base_bytes.store(base_size_.load(std::memory_order_relaxed), std::memory_order_relaxed);
}

AOFManager::~AOFManager() {
    stop_sync_worker();
    std::lock_guard lock(mutex_);
    if (fd_ >= 0) {
        if (sync_policy_ != SyncPolicy::Off && size_.load(std::memory_order_relaxed) > synced_) {
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

void AOFManager::publish_sync_locked() const {
    const uint64_t size = size_.load(std::memory_order_relaxed);
    global_metrics.aof_synced_bytes.store(synced_, std::memory_order_relaxed);
    global_metrics.aof_pending_bytes.store(size > synced_ ? size - synced_ : 0, std::memory_order_relaxed);
    global_metrics.aof_generation.store(generation_, std::memory_order_relaxed);
    global_metrics.aof_last_sync_epoch_ms.store(static_cast<uint64_t>(std::max(0LL, last_sync_epoch_ms_)),
                                                std::memory_order_relaxed);
}

void AOFManager::note_append_wait(const std::chrono::steady_clock::time_point start) const {
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
    global_metrics.aof_append_lock_wait_us.store(static_cast<uint64_t>(us < 0 ? 0 : us), std::memory_order_relaxed);
    if (us >= 50000) global_metrics.aof_append_lock_waits_slow.fetch_add(1, std::memory_order_relaxed);
}

void AOFManager::bump_generation_locked() {
    ++generation_;
}

bool AOFManager::sync_fd_delayed(const int fd) const {
    if (sync_delay_ms_ > 0) std::this_thread::sleep_for(std::chrono::milliseconds(sync_delay_ms_));
    for (;;) {
        if (::fdatasync(fd) == 0) return true;
        if (errno != EINTR) return false;
    }
}

bool AOFManager::sync_locked(const int fd) {
    const auto t0 = std::chrono::steady_clock::now();
    if (!sync_fd_delayed(fd)) {
        healthy_ = false;
        fprintf(stderr, "AOF: fdatasync a esuat: %s (persistenta suspecta!)\n", strerror(errno));
        return false;
    }
    const auto dur_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
    global_metrics.aof_sync_duration_us.store(static_cast<uint64_t>(dur_us < 0 ? 0 : dur_us), std::memory_order_relaxed);
    last_sync_epoch_ms_ = now_ms();
    if (fd == fd_) synced_ = size_.load(std::memory_order_relaxed);
    publish_sync_locked();
    return true;
}

void AOFManager::stop_sync_worker() {
    {
        std::lock_guard lock(mutex_);
        sync_stop_ = true;
    }
    sync_cv_.notify_all();
    if (sync_thread_.joinable()) sync_thread_.join();
}

void AOFManager::sync_worker() {
    std::unique_lock lock(mutex_);
    while (true) {
        if (fd_ < 0 || size_.load(std::memory_order_relaxed) <= synced_) {
            if (sync_stop_) break;
            sync_cv_.wait(lock, [&] {
                return sync_stop_ || (fd_ >= 0 && size_.load(std::memory_order_relaxed) > synced_);
            });
            continue;
        }
        const auto now = std::chrono::steady_clock::now();
        if (!sync_stop_ && now < next_attempt_) {
            sync_cv_.wait_until(lock, next_attempt_, [&] { return sync_stop_; });
            continue;
        }

        const uint64_t generation = generation_;
        const uint64_t offset = size_.load(std::memory_order_relaxed);
        const int dupfd = fd_ >= 0 ? ::fcntl(fd_, F_DUPFD_CLOEXEC, 0) : -1;
        const auto started = std::chrono::steady_clock::now();
        lock.unlock();

        global_metrics.aof_sync_inflight.store(1, std::memory_order_release);
        const bool ok = dupfd >= 0 && sync_fd_delayed(dupfd);
        const int sync_errno = errno;
        const auto dur_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started).count();
        if (dupfd >= 0) ::close(dupfd);

        lock.lock();
        global_metrics.aof_sync_inflight.store(0, std::memory_order_release);
        global_metrics.aof_sync_duration_us.store(static_cast<uint64_t>(dur_us < 0 ? 0 : dur_us),
                                                  std::memory_order_relaxed);
        if (dur_us > sync_interval_ms_ * 1000) {
            global_metrics.aof_sync_overruns.fetch_add(1, std::memory_order_relaxed);
        }
        // doar generatia capturata: un fd inlocuit intre timp nu marcheaza fisierul nou
        if (dupfd >= 0 && generation == generation_) {
            if (!ok) {
                healthy_ = false;
                fprintf(stderr, "AOF: fdatasync a esuat: %s (persistenta suspecta!)\n", strerror(sync_errno));
            } else if (offset > synced_) {
                synced_ = offset;
                last_sync_epoch_ms_ = now_ms();
            }
        }
        publish_sync_locked();

        auto next = started + std::chrono::milliseconds(sync_interval_ms_);
        const auto finished = std::chrono::steady_clock::now();
        if (next < finished) next = finished;
        next_attempt_ = next;
    }
}

bool AOFManager::open_file() {
    std::lock_guard lock(mutex_);
    if (fd_ >= 0) return true;

    fd_ = ::open(kAofPath, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd_ < 0) {
        healthy_ = false;
        fprintf(stderr, "AOF: nu pot deschide %s: %s\n", kAofPath, strerror(errno));
        return false;
    }

    struct stat sb{};
    const bool has_size = ::fstat(fd_, &sb) == 0;
    bump_generation_locked();
    // fisier nou: header de format, ca deschiderile viitoare sa nu mai
    // ghiceasca nimic din continut
    if (has_size && sb.st_size == 0) {
        size_ = 0;
        if (!fsutil::write_all(fd_, kAofHeader, sizeof(kAofHeader) - 1)) {
            fprintf(stderr, "AOF: nu pot scrie headerul v3\n");
            healthy_ = false;
            ::close(fd_);
            fd_ = -1;
            return false;
        }
        size_ = sizeof(kAofHeader) - 1;
        if (!sync_locked(fd_)) {
            fprintf(stderr, "AOF: nu pot scrie headerul v3\n");
            ::close(fd_);
            fd_ = -1;
            return false;
        }
        fsutil::sync_dir();
        publish_sync_locked();
    } else {
        // continutul deja pe disc e baza; coada pierduta la crash e contractul everysec
        size_ = has_size && sb.st_size > 0 ? static_cast<uint64_t>(sb.st_size) : 0;
        synced_ = size_.load(std::memory_order_relaxed);
        publish_sync_locked();
    }

    // ca Redis: dimensiunea de la pornire e baza pentru rescrierea automata
    base_size_ = size_.load();
    publish_sizes();
    if (sync_policy_ == SyncPolicy::Everysec) {
        printf("AOF: format v3, politica durabilitate: everysec (thread dedicat, interval %lld ms)\n",
               sync_interval_ms_);
    } else {
        printf("AOF: format v3, politica durabilitate: %s\n", policy_name());
    }
    return true;
}

bool AOFManager::append(const std::string& room_name, const std::vector<std::string>& args,
                        const long long expire_at, const uint64_t timestamp_ms, const uint32_t node_id) {
    const auto wait_t0 = std::chrono::steady_clock::now();
    std::lock_guard lock(mutex_);
    note_append_wait(wait_t0);
    if (replaying_) return false;

    AofRecord rec;
    rec.room = room_name;
    rec.args = args;
    rec.expire_at = expire_at;
    rec.timestamp_ms = timestamp_ms;
    rec.node_id = node_id;
    const std::string data = encode_record(rec);

    if (fd_ < 0) {
        fd_ = ::open(kAofPath, O_WRONLY | O_CREAT | O_APPEND, 0600);
        if (fd_ < 0) {
            healthy_ = false;
            return false;
        }
        bump_generation_locked();
        synced_ = 0;
        healthy_ = true;
    }

    if (!fsutil::write_all(fd_, data.data(), data.size())) {
        healthy_ = false;
        fprintf(stderr, "AOF: scriere esuata: %s (mutatia exista doar in RAM!)\n", strerror(errno));
        ::close(fd_);
        fd_ = -1;
        bump_generation_locked();
        publish_sync_locked();
        return false;
    }
    size_.fetch_add(data.size(), std::memory_order_relaxed);
    publish_sizes();
    publish_sync_locked();

    switch (sync_policy_) {
        case SyncPolicy::Always:
            // ack-ul asteapta fdatasync-ul; lock-ul ramane prins intentionat
            if (!sync_locked(fd_)) return false;
            break;
        case SyncPolicy::Everysec:
            // thread-ul dedicat sincronizeaza in afara acestui lock. Un fdatasync
            // lent nu are voie sa intarzie append-ul. Octetii scrisi dupa offsetul
            // capturat raman pending pana la sincronizarea urmatoare.
            sync_cv_.notify_one();
            break;
        default:
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
        resp::Status st;
        while ((st = resp::parse(buf, pos, &rec_end, &tokens)) == resp::Status::NeedMore) {
            if (!fill()) break; // EOF in mijlocul unei inregistrari
        }

        if (st == resp::Status::NeedMore) {
            if (pos < buf.size()) truncate_tail(compacted + pos);
            break; // istoricul complet ramane neatins
        }
        if (st == resp::Status::Malformed) {
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
    base_size_ = size_.load(); // orice incercare reseteaza baza (vezi rewrite_due)
    publish_sizes();
    rewrite_fd_ = ::open("appendonly.aof.tmp", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (rewrite_fd_ < 0) {
        fprintf(stderr, "AOF: rescriere: nu pot crea fisierul temporar: %s\n", strerror(errno));
        return false;
    }
    if (!fsutil::write_all(rewrite_fd_, kAofHeader, sizeof(kAofHeader) - 1)) {
        fprintf(stderr, "AOF: rescriere: headerul nu a putut fi scris\n");
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
        fprintf(stderr, "AOF: rescriere: scriere esuata: %s\n", strerror(errno));
        return false;
    }
    return true;
}

void AOFManager::abort_locked() {
    last_tail_ = 0;
    if (rewrite_fd_ >= 0) {
        ::close(rewrite_fd_);
        rewrite_fd_ = -1;
    }
    ::unlink("appendonly.aof.tmp");
    // fisierul original (vechi) ramane neatins: rescrierea poate fi reincercata
}

void AOFManager::abort_rewrite() {
    std::lock_guard lock(mutex_);
    abort_locked();
}

bool AOFManager::append_tail(const uint64_t from) {
    std::lock_guard lock(mutex_);
    if (rewrite_fd_ < 0) return false;
    const int in = ::open(kAofPath, O_RDONLY);
    if (in < 0) return false;

    std::string buf(1 << 16, '\0');
    const uint64_t end = size_.load(std::memory_order_relaxed);
    last_tail_ = end >= from ? end - from : 0;
    bool ok = true;
    for (uint64_t off = from; ok && off < end;) {
        const size_t n = static_cast<size_t>(std::min<uint64_t>(buf.size(), end - off));
        ok = fsutil::pread_all(in, buf.data(), n, static_cast<off_t>(off)) && fsutil::write_all(rewrite_fd_, buf.data(), n);
        off += n;
    }
    ::close(in);
    if (!ok) fprintf(stderr, "AOF: rescriere: coada nu a putut fi copiata: %s\n", strerror(errno));
    return ok;
}

bool AOFManager::commit_rewrite() {
    std::lock_guard lock(mutex_);
    if (rewrite_fd_ < 0) return false;

    struct stat sb{};
    const uint64_t written = ::fstat(rewrite_fd_, &sb) == 0 ? static_cast<uint64_t>(sb.st_size) : 0;
    if (!fsutil::sync_fd(rewrite_fd_)) {
        fprintf(stderr, "AOF: rescriere: fdatasync esuat: %s\n", strerror(errno));
        abort_locked();
        return false;
    }
    if (::close(rewrite_fd_) != 0) {
        healthy_ = false;
        fprintf(stderr, "AOF: rescriere: close esuat: %s\n", strerror(errno));
        rewrite_fd_ = -1;
        ::unlink("appendonly.aof.tmp");
        return false;
    }
    rewrite_fd_ = -1;

    // substitutie atomica: ori vechiul fisier, ori cel nou v3, niciodata unul partial
    if (::rename("appendonly.aof.tmp", kAofPath) != 0) {
        healthy_ = false;
        fprintf(stderr, "AOF: rescriere: rename esuat: %s\n", strerror(errno));
        ::unlink("appendonly.aof.tmp");
        return false;
    }
    fsutil::sync_dir();

    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    fd_ = ::open(kAofPath, O_WRONLY | O_CREAT | O_APPEND, 0600);
    // generatie noua: o sincronizare in zbor pe fd-ul vechi nu marcheaza fisierul nou
    bump_generation_locked();
    healthy_ = fd_ >= 0;
    needs_rewrite_ = false;
    size_ = written;
    // baza e instantaneul, nu fisierul cu coada: daca scrierile din timpul
    // copilului au umflat rezultatul, urmatoarea comanda mai rescrie o data
    const uint64_t snapshot = written >= last_tail_ ? written - last_tail_ : written;
    base_size_ = snapshot == 0 ? 1 : snapshot;
    last_tail_ = 0;
    // rewrite_fd_ a fost deja fdatasync-uit inainte de rename; acel inode e durabil
    synced_ = healthy_ ? written : 0;
    last_sync_epoch_ms_ = healthy_ ? now_ms() : last_sync_epoch_ms_;
    publish_sync_locked();
    publish_sizes();
    sync_cv_.notify_all();
    printf("AOF: rescriere finalizata (%llu octeti)\n", static_cast<unsigned long long>(written));
    return healthy_;
}
