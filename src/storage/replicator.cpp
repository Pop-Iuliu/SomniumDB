#include "replicator.h"
#include "fs_util.h"
#include "../core/resp.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <utility>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

namespace {
    constexpr char kAofPath[] = "appendonly.aof";
    constexpr uint64_t kMaxBatchBytes = 4ull << 20;

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
            fd = ::socket(a->ai_family, a->ai_socktype | SOCK_CLOEXEC, a->ai_protocol);
            if (fd >= 0 && ::connect(fd, a->ai_addr, a->ai_addrlen) != 0) {
                ::close(fd);
                fd = -1;
            }
        }
        freeaddrinfo(res);
        if (fd >= 0) {
            // un peer blocat nu tine thread-ul pe loc la nesfarsit
            const timeval tv{2, 0};
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        }
        return fd;
    }
} // namespace

Replicator::Replicator() {
    if (const char* env = getenv("SOMNIUM_NODE_ID")) {
        uint64_t id = 0;
        if (fsutil::parse_u64(env, &id) && id >= 1 && id <= UINT32_MAX) node_id_ = static_cast<uint32_t>(id);
    }

    const char* env = getenv("SOMNIUM_PEERS");
    const std::string list = env ? env : "";
    for (size_t start = 0; start < list.size();) {
        const size_t comma = std::min(list.find(',', start), list.size());
        const std::string item = list.substr(start, comma - start);
        start = comma + 1;

        const size_t colon = item.rfind(':');
        if (colon == std::string::npos || colon == 0 || colon + 1 == item.size()) {
            fprintf(stderr, "SOMNIUM_PEERS: intrare invalida '%s' (astept host:port)\n", item.c_str());
            continue;
        }
        Peer peer;
        peer.host = item.substr(0, colon);
        peer.port = item.substr(colon + 1);
        peer.state_file = "replication_" + peer.host + "_" + peer.port + ".offset";
        std::ifstream(peer.state_file) >> peer.inode >> peer.offset; // absent: de la zero
        peers_.push_back(std::move(peer));
    }
}

Replicator::~Replicator() {
    running_ = false;
    if (worker_.joinable()) worker_.join();
    for (const Peer& peer : peers_) {
        if (peer.fd >= 0) ::close(peer.fd);
    }
}

void Replicator::start() {
    if (peers_.empty()) return;
    printf("Replicare: nodul %u trimite catre %zu peer(i)\n", node_id_, peers_.size());
    running_ = true;
    worker_ = std::thread([this] {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            for (Peer& peer : peers_) ship(peer);
        }
    });
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

void Replicator::ship(Peer& peer) {
    if (peer.fd < 0) {
        peer.fd = connect_to(peer.host, peer.port);
        peer.room.clear(); // conexiune noua: camera selectata e iar "default"
        if (peer.fd < 0) return;
    }

    const int aof = ::open(kAofPath, O_RDONLY | O_CLOEXEC);
    if (aof < 0) return;
    struct stat sb{};
    if (::fstat(aof, &sb) != 0) {
        ::close(aof);
        return;
    }
    const uint64_t size = static_cast<uint64_t>(sb.st_size);
    if (static_cast<uint64_t>(sb.st_ino) != peer.inode || peer.offset > size) {
        // alt fisier (rescriere S8) sau unul trunchiat: retrimitem de la inceput,
        // merge-urile sunt idempotente
        peer.inode = static_cast<uint64_t>(sb.st_ino);
        peer.offset = 0;
    }

    std::string buf(std::min(size - peer.offset, kMaxBatchBytes), '\0');
    bool read_ok = fsutil::pread_all(aof, buf.data(), buf.size(), static_cast<off_t>(peer.offset));

    // comenzile de trimis si, dupa fiecare inregistrare, cate raspunsuri trebuie
    // confirmate pana la ea si offsetul de dupa ea
    std::string out;
    std::string room = peer.room;
    std::vector<std::pair<size_t, uint64_t>> marks;
    const auto build = [&] {
        size_t expected = 0, pos = 0, end = 0;
        std::vector<std::string> t;
        out.clear();
        room = peer.room;
        marks.clear();
        while (resp::parse(buf, pos, &end, &t) == resp::Status::Complete) {
            pos = end;
            // v3: [room, CMD, args..., META, expire, ts, node]; headerul, DEL/EXPIRE
            // si inregistrarile venite de la alti noduri doar avanseaza offsetul
            const size_t n = t.size();
            uint64_t node = 0;
            if (n >= 8 && t[n - 4] == "SOMNIUM-META" && fsutil::parse_u64(t[n - 1], &node) && node == node_id_) {
                std::string cmd = t[1];
                std::ranges::transform(cmd, cmd.begin(), ::toupper);
                // SET si CRDTMERGE: o pereche; MSET: toate perechile, cu aceeasi versiune
                const size_t pairs_end = cmd == "MSET" ? n - 4 : (cmd == "SET" || cmd == "CRDTMERGE") ? 4 : 0;
                for (size_t i = 2; i + 1 < pairs_end; i += 2) {
                    if (t[0] != room) {
                        out += encode({"ROOM", t[0]});
                        ++expected;
                        room = t[0];
                    }
                    out += encode({"CRDTMERGE", t[i], t[i + 1], t[n - 2], t[n - 1], t[n - 3]});
                    ++expected;
                }
            }
            marks.emplace_back(expected, peer.offset + pos);
        }
        return expected;
    };
    size_t expected = read_ok ? build() : 0;
    if (read_ok && marks.empty() && buf.size() < size - peer.offset) {
        // o singura inregistrare mai mare decat un batch: o citim intreaga
        buf.assign(size - peer.offset, '\0');
        read_ok = fsutil::pread_all(aof, buf.data(), buf.size(), static_cast<off_t>(peer.offset));
        expected = read_ok ? build() : 0;
    }
    ::close(aof);
    if (marks.empty()) return;

    size_t acked = expected;
    if (expected > 0) {
        if (!deliver(peer, out, expected, &acked)) {
            ::close(peer.fd);
            peer.fd = -1;
            acked = 0;
        }
        // un raspuns de eroare (ex. ROOMS FULL la peer) lasa camera incerta
        peer.room = acked == expected ? room : "";
    }

    const uint64_t before = peer.offset;
    for (const auto& [replies, offset] : marks) {
        if (replies > acked) break; // de aici incolo, la urmatoarea caravana
        peer.offset = offset;
    }
    if (peer.offset != before) save(peer);
}

bool Replicator::deliver(Peer& peer, const std::string& out, const size_t expected, size_t* acked) {
    if (!fsutil::write_all(peer.fd, out.data(), out.size())) return false;

    // raspunsurile la ROOM si CRDTMERGE sunt linii simple: "+..." sau "-..."
    std::string in;
    size_t pos = 0;
    bool failed = false;
    *acked = 0;
    for (size_t seen = 0; seen < expected;) {
        const size_t crlf = in.find("\r\n", pos);
        if (crlf == std::string::npos) {
            char chunk[4096];
            const ssize_t n = ::recv(peer.fd, chunk, sizeof(chunk), 0);
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
