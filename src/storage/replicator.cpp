#include "replicator.h"
#include "fs_util.h"
#include "../core/resp.h"
#include "../server/metrics.h"
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <numeric>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <utility>
#include <fcntl.h>
#include <netdb.h>
#include <openssl/err.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

namespace {
    constexpr char kAofPath[] = "appendonly.aof";
    constexpr uint64_t kMaxBatchBytes = 4ull << 20;
    constexpr long long kTickMs = 500;
    constexpr long long kMaxBackoffMs = 30000;
    constexpr int kIoTimeoutMs = 2000;
    constexpr size_t kBeatWindow = 100;

    long long steady_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    std::string encode(const std::vector<std::string>& args) {
        std::string out = "*" + std::to_string(args.size()) + "\r\n";
        for (const std::string& a : args) out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
        return out;
    }

    int connect_to(const std::string& host, const std::string& port) {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* res = nullptr;
        if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) return -1;

        int fd = -1;
        for (const addrinfo* a = res; a && fd < 0; a = a->ai_next) {
            // connect neblocant, marginit de poll: un peer care inghite SYN-urile
            // ar tine altfel thread-ul ~2 minute (tcp_syn_retries)
            fd = ::socket(a->ai_family, a->ai_socktype | SOCK_CLOEXEC | SOCK_NONBLOCK, a->ai_protocol);
            if (fd < 0) continue;
            bool connected = ::connect(fd, a->ai_addr, a->ai_addrlen) == 0;
            if (!connected && errno == EINPROGRESS) {
                pollfd pfd{fd, POLLOUT, 0};
                int err = 0;
                socklen_t len = sizeof(err);
                connected = ::poll(&pfd, 1, kIoTimeoutMs) == 1 &&
                            getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0;
            }
            if (!connected) {
                ::close(fd);
                fd = -1;
            }
        }
        freeaddrinfo(res);
        if (fd >= 0) {
            // de aici I/O blocant, dar marginit si el in timp
            ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) & ~O_NONBLOCK);
            const timeval tv{kIoTimeoutMs / 1000, 0};
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        }
        return fd;
    }
} // namespace

Replicator::Replicator(const std::atomic<uint64_t>& frontier, const Config& cfg, SSL_CTX* tls)
    : node_id_(cfg.node_id), password_(cfg.password), tls_(tls), frontier_(frontier) {
    for (const auto& [host, port] : cfg.peers) {
        Peer peer;
        peer.host = host;
        peer.port = port;
        peer.state_file = "replication_" + peer.host + "_" + peer.port + ".offset";
        std::ifstream(peer.state_file) >> peer.inode >> peer.offset; // absent: de la zero
        peers_.push_back(std::move(peer));
    }
}

Replicator::~Replicator() {
    running_ = false;
    for (std::thread& worker : workers_) worker.join();
    for (Peer& peer : peers_) disconnect(peer);
}

void Replicator::disconnect(Peer& peer) {
    SSL_free(peer.ssl); // nullptr: nimic
    peer.ssl = nullptr;
    if (peer.fd >= 0) ::close(peer.fd);
    peer.fd = -1;
    ERR_clear_error(); // erorile TLS ale conexiunii inchise nu mai conteaza
}

void Replicator::start() {
    if (peers_.empty()) return;
    printf("Replicare: nodul %u trimite catre %zu peer(i)\n", node_id_, peers_.size());
    running_ = true;
    for (Peer& peer : peers_) {
        publish(peer);
        workers_.emplace_back([this, &peer] {
            long long delay = kTickMs;
            while (running_) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay));
                if (ship(peer)) {
                    beat(peer);
                    delay = kTickMs;
                } else {
                    delay = std::min(delay * 2, kMaxBackoffMs); // backoff exponential
                }
                publish(peer);
            }
        });
    }
}

void Replicator::beat(Peer& peer) {
    const long long now = steady_ms();
    if (peer.last_beat_ms != 0) {
        peer.intervals.push_back(static_cast<double>(now - peer.last_beat_ms));
        if (peer.intervals.size() > kBeatWindow) peer.intervals.pop_front();
    }
    peer.last_beat_ms = now;
}

void Replicator::publish(const Peer& peer) {
    double mean = static_cast<double>(kTickMs), stddev = 0;
    if (!peer.intervals.empty()) {
        mean = std::accumulate(peer.intervals.begin(), peer.intervals.end(), 0.0) / peer.intervals.size();
        double var = 0;
        for (const double d : peer.intervals) var += (d - mean) * (d - mean);
        stddev = std::sqrt(var / peer.intervals.size());
    }
    publish_peer(peer.host + ":" + peer.port, mean, stddev, peer.last_beat_ms, peer.lag);
}

void Replicator::save(const Peer& peer) {
    const std::string tmp = peer.state_file + ".tmp";
    {
        std::ofstream f(tmp, std::ios::trunc);
        f << peer.inode << ' ' << peer.offset << '\n';
        if (!f) return;
    }
    ::rename(tmp.c_str(), peer.state_file.c_str());
}

bool Replicator::ship(Peer& peer) {
    // frontiera se citeste INAINTE de AOF: tot ce e sub ea e deja in fisierul citit
    const uint64_t frontier = frontier_.load();
    const int aof = ::open(kAofPath, O_RDONLY | O_CLOEXEC);
    if (aof < 0) return false;
    struct stat sb{};
    if (::fstat(aof, &sb) != 0) {
        ::close(aof);
        return false;
    }
    const uint64_t size = static_cast<uint64_t>(sb.st_size);
    if (static_cast<uint64_t>(sb.st_ino) != peer.inode || peer.offset > size) {
        // alt fisier (rescriere S8) sau unul trunchiat: retrimitem de la inceput,
        // merge-urile sunt idempotente
        peer.inode = static_cast<uint64_t>(sb.st_ino);
        peer.offset = 0;
    }
    peer.lag = size - peer.offset; // si pentru un peer inaccesibil
    publish(peer);                 // vizibil si cat connect-ul asteapta (pana la kIoTimeoutMs)

    if (peer.fd < 0) {
        peer.fd = connect_to(peer.host, peer.port);
        peer.room.clear(); // conexiune noua: camera selectata e iar "default"
        if (peer.fd >= 0 && (!secure(peer) || !authenticate(peer))) disconnect(peer);
        if (peer.fd < 0) {
            ::close(aof);
            return false;
        }
    }

    std::string buf(std::min(size - peer.offset, kMaxBatchBytes), '\0');
    bool read_ok = fsutil::pread_all(aof, buf.data(), buf.size(), static_cast<off_t>(peer.offset));

    // comenzile de trimis si, dupa fiecare inregistrare, cate raspunsuri trebuie
    // confirmate pana la ea si offsetul de dupa ea
    std::string out;
    std::string room = peer.room;
    std::vector<std::pair<size_t, uint64_t>> marks;
    bool misaligned = false; // prima inregistrare de la offset nu e RESP valid
    const auto build = [&] {
        size_t expected = 0, pos = 0, end = 0;
        std::vector<std::string> t;
        out.clear();
        room = peer.room;
        marks.clear();
        resp::Status st;
        while ((st = resp::parse(buf, pos, &end, &t)) == resp::Status::Complete) {
            pos = end;
            // v3: [room, CMD, args..., META, expire, ts, node]; headerul si
            // inregistrarile venite de la alti noduri doar avanseaza offsetul
            const size_t n = t.size();
            uint64_t node = 0;
            if (n >= 7 && t[n - 4] == "SOMNIUM-META" && fsutil::parse_u64(t[n - 1], &node) && node == node_id_) {
                std::string cmd = t[1];
                std::ranges::transform(cmd, cmd.begin(), ::toupper);
                const auto send = [&](const std::vector<std::string>& command) {
                    if (t[0] != room) {
                        out += encode({"ROOM", t[0]});
                        ++expected;
                        room = t[0];
                    }
                    out += encode(command);
                    ++expected;
                };
                if (cmd == "DEL" || cmd == "CRDTDEL") {
                    send({"CRDTDEL", t[2], t[n - 2], t[n - 1]});
                } else {
                    // SET si CRDTMERGE: o pereche; MSET: toate perechile, cu aceeasi versiune
                    const size_t pairs_end = cmd == "MSET" ? n - 4 : (cmd == "SET" || cmd == "CRDTMERGE") ? 4 : 0;
                    for (size_t i = 2; i + 1 < pairs_end; i += 2) {
                        send({"CRDTMERGE", t[i], t[i + 1], t[n - 2], t[n - 1], t[n - 3]});
                    }
                }
            }
            marks.emplace_back(expected, peer.offset + pos);
        }
        misaligned = pos == 0 && st == resp::Status::Malformed;
        return expected;
    };
    size_t expected = read_ok ? build() : 0;
    if (read_ok && marks.empty() && !misaligned && buf.size() < size - peer.offset) {
        // o singura inregistrare mai mare decat un batch: o citim intreaga
        buf.assign(size - peer.offset, '\0');
        read_ok = fsutil::pread_all(aof, buf.data(), buf.size(), static_cast<off_t>(peer.offset));
        expected = read_ok ? build() : 0;
    }
    ::close(aof);
    if (read_ok && misaligned && peer.offset > 0) {
        // offsetul nu cade pe o inregistrare: alt fisier cu acelasi inode (ext4 refoloseste
        // inode-ul eliberat, deci doua rescrieri cat peer-ul e oprit il pot aduce inapoi).
        // Fara asta replicarea se oprea pentru totdeauna, cu heartbeat-uri sanatoase si lag
        // crescator. Retrimitem de la inceput: merge-urile sunt idempotente.
        fprintf(stderr, "Replicare: %s:%s offset %llu nu e inceputul unei inregistrari; retrimit de la 0\n",
                peer.host.c_str(), peer.port.c_str(), static_cast<unsigned long long>(peer.offset));
        peer.offset = 0;
        save(peer);
    }

    // un batch care a ajuns la capatul AOF-ului se incheie cu frontiera (si tine loc
    // de heartbeat); altfel, fara nimic de trimis, un PING. Marcajele nu le numara.
    const bool reached_end = read_ok && (marks.empty() ? peer.offset : marks.back().second) == size;
    size_t total = expected;
    if (reached_end) {
        out += encode({"REPLFRONTIER", std::to_string(node_id_), std::to_string(frontier)});
        ++total;
    } else if (expected == 0) {
        out = encode({"PING"});
        total = 1;
    }

    size_t acked = 0;
    const bool alive = deliver(peer, out, total, &acked);
    if (!alive) {
        disconnect(peer);
        acked = 0;
    }
    // un raspuns de eroare (ex. ROOMS FULL la peer) lasa camera incerta
    if (expected > 0) peer.room = acked == expected ? room : "";

    const uint64_t before = peer.offset;
    for (const auto& [replies, offset] : marks) {
        if (replies > acked) break; // de aici incolo, la urmatoarea caravana
        peer.offset = offset;
    }
    if (peer.offset != before) save(peer);
    peer.lag = size - peer.offset;
    return alive;
}

// SEC-16: cu TLS, peer-ul trebuie sa prezinte certificatul clusterului, acelasi pe
// toate nodurile (ca parola): fara CA de configurat si fara verificarea numelui
bool Replicator::secure(Peer& peer) {
    if (!tls_) return true;
    peer.ssl = SSL_new(tls_);
    bool ok = peer.ssl && SSL_set_fd(peer.ssl, peer.fd) == 1 && SSL_connect(peer.ssl) == 1;
    if (ok) {
        X509* theirs = SSL_get1_peer_certificate(peer.ssl);
        ok = theirs && X509_cmp(theirs, SSL_CTX_get0_certificate(tls_)) == 0;
        X509_free(theirs);
    }
    if (!ok) fprintf(stderr, "Replicare: %s:%s nu prezinta certificatul clusterului (TLS)\n", peer.host.c_str(), peer.port.c_str());
    return ok;
}

// prima comanda pe o conexiune noua, cand nodurile au parola
bool Replicator::authenticate(Peer& peer) {
    if (password_.empty()) return true;
    size_t acked = 0;
    if (deliver(peer, encode({"AUTH", password_}), 1, &acked) && acked == 1) return true;
    fprintf(stderr, "Replicare: %s:%s a refuzat autentificarea\n", peer.host.c_str(), peer.port.c_str());
    return false;
}

bool Replicator::deliver(Peer& peer, const std::string& out, const size_t expected, size_t* acked) {
    const bool sent = peer.ssl ? SSL_write(peer.ssl, out.data(), static_cast<int>(out.size())) > 0
                               : fsutil::write_all(peer.fd, out.data(), out.size());
    if (!sent) return false;

    // raspunsurile la ROOM si CRDTMERGE sunt linii simple: "+..." sau "-..."
    std::string in;
    size_t pos = 0;
    bool failed = false;
    *acked = 0;
    for (size_t seen = 0; seen < expected;) {
        const size_t crlf = in.find("\r\n", pos);
        if (crlf == std::string::npos) {
            char chunk[4096];
            const ssize_t n = peer.ssl ? SSL_read(peer.ssl, chunk, sizeof(chunk)) : ::recv(peer.fd, chunk, sizeof(chunk), 0);
            if (n <= 0) return false;
            in.append(chunk, static_cast<size_t>(n));
            continue;
        }
        failed = failed || in[pos] != '+';
        if (!failed) ++*acked;
        ++seen;
        pos = crlf + 2;
    }
    return true;
}
