#include "snapshot_manager.h"
#include "fs_util.h"
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <fstream>
#include <unordered_map>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {
    constexpr char SNAPSHOT_MAGIC[4] = {'R', 'S', 'N', 'P'};
    constexpr uint32_t SNAPSHOT_VERSION = 2;

    void destroy_all_records(Room& room, PoolAllocator<Record, 1024>& pool) {
        for (const auto& [key, rec] : room.keys) {
            pool.destroy(rec);
        }
        room.keys.clear();
    }
} // namespace

bool SnapshotManager::has_snapshot(const std::string& room_name) {
    struct stat sb{};
    const std::string filename = "room_" + room_name + ".bin";
    // doar fisiere regulate conteaza; un director cu acelasi nume nu e un
    // snapshot (si poate servi drept injectie de esec la rename)
    return ::stat(filename.c_str(), &sb) == 0 && S_ISREG(sb.st_mode);
}

bool SnapshotManager::wakeup_room(Room& room, PoolAllocator<Record, 1024>& pool) {
    const std::string filename = "room_" + room.name + ".bin";
    struct stat sb{};
    if (::stat(filename.c_str(), &sb) != 0 || !S_ISREG(sb.st_mode)) {
        room.hibernated = false;
        return false;
    }

    std::string data(static_cast<size_t>(sb.st_size), '\0');
    {
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open() || sb.st_size > 0 &&
            !file.read(data.data(), static_cast<std::streamsize>(sb.st_size))) {
            // fisierul exista dar nu poate fi citit: il pastram, nu il stergem
            room.hibernated = false;
            return false;
        }
    }

    // stare temporara: room-ul curent e atins doar dupa o incarcare completa
    std::unordered_map<std::string, Record*> loaded;
    bool corrupt = false;
    size_t off = 0;

    const auto need = [&](const size_t n) {
        return off + n <= data.size();
    };
    const auto read_bytes = [&](void* dst, const size_t n) {
        std::memcpy(dst, data.data() + off, n);
        off += n;
    };

    uint8_t version_bytes[4] = {};
    bool is_v2 = false;
    if (!need(8)) {
        corrupt = true;
    } else {
        is_v2 = std::memcmp(data.data(), SNAPSHOT_MAGIC, sizeof(SNAPSHOT_MAGIC)) == 0;
        if (is_v2) {
            std::memcpy(version_bytes, data.data() + 4, 4);
            uint32_t version = 0;
            std::memcpy(&version, version_bytes, sizeof(version));
            // headerul v2 promite si o versiune: o validam explicit, nu ne bazam
            // pe "magic-ul coincide, restul o fi bine"
            if (version != SNAPSHOT_VERSION) corrupt = true;
        }
    }

    size_t num_records = 0;
    if (!corrupt) {
        if (is_v2) {
            // magic(4) + version(4) sunt deja validati: sarim peste ei
            off = 8;
            read_bytes(&num_records, sizeof(num_records));
        } else {
            // format v1 (legacy): primii 8 bytes sunt num_records, fara magic
            std::memcpy(&num_records, data.data(), sizeof(num_records));
            off = sizeof(num_records);
        }
        // fiecare inregistrare are minim ~26 de bytes; un contor absurd e corupt
        if (num_records > data.size()) corrupt = true;
    }

    while (!corrupt && loaded.size() < num_records) {
        size_t klen = 0;
        size_t vlen = 0;

        if (!need(sizeof(klen))) { corrupt = true; break; }
        read_bytes(&klen, sizeof(klen));
        if (!need(klen)) { corrupt = true; break; }
        std::string key(data.data() + off, klen);
        off += klen;

        if (!need(sizeof(vlen))) { corrupt = true; break; }
        read_bytes(&vlen, sizeof(vlen));
        if (!need(vlen)) { corrupt = true; break; }
        std::string val(data.data() + off, vlen);
        off += vlen;

        long long expire_at = 0;
        uint64_t timestamp_ms = 0;
        uint32_t node_id = 1;

        if (!need(sizeof(expire_at))) { corrupt = true; break; }
        read_bytes(&expire_at, sizeof(expire_at));

        if (is_v2) {
            if (!need(sizeof(timestamp_ms) + sizeof(node_id))) { corrupt = true; break; }
            read_bytes(&timestamp_ms, sizeof(timestamp_ms));
            read_bytes(&node_id, sizeof(node_id));
        }

        Record* rec = pool.construct(std::move(val), expire_at);
        rec->timestamp_ms = timestamp_ms;
        rec->node_id = node_id;
        // ultima aparitie a cheii in fisier castiga (fisierul e generat de noi,
        // dar validarea tot o impune)
        if (const auto it = loaded.find(key); it != loaded.end()) {
            pool.destroy(it->second);
            it->second = rec;
        } else {
            loaded.emplace(std::move(key), rec);
        }
    }

    if (corrupt) {
        for (const auto& [key, rec] : loaded) pool.destroy(rec);
        const std::string dst = filename + ".corrupt." + std::to_string(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        if (::rename(filename.c_str(), dst.c_str()) != 0) {
            fprintf(stderr, "Snapshot corupt pentru camera '%s' si nu poate fi pastrat deoparte: %s\n",
                    room.name.c_str(), strerror(errno));
        } else {
            printf("Snapshot corupt pentru camera '%s'; pastrat pentru diagnostic: %s\n",
                   room.name.c_str(), dst.c_str());
        }
        room.hibernated = false;
        return false;
    }

    // abia acum, dupa incarcare valida, eliberam starea veche si o substituem
    destroy_all_records(room, pool);
    room.keys = std::move(loaded);
    room.hibernated = false;
    return true;
}

HibernateResult SnapshotManager::hibernate_room(Room& room, PoolAllocator<Record, 1024>& pool, size_t* written) {
    const std::string filename = "room_" + room.name + ".bin";
    const std::string tmpname = filename + ".tmp";

    const int fd = ::open(tmpname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "Snapshot '%s': nu pot crea fisierul temporar: %s\n", room.name.c_str(), strerror(errno));
        return HibernateResult::Failure;
    }

    const uint64_t num_records = room.keys.size();

    // scriere in buffer cu flush periodic: putine syscall-uri, fara tampon
    // de marimea intregii camere in memorie
    std::string buf;
    buf.reserve(1 << 16);
    bool ok = true;
    const auto flush_buf = [&]() {
        if (!ok || buf.empty()) return;
        if (!fsutil::write_all(fd, buf.data(), buf.size())) {
            fprintf(stderr, "Snapshot '%s': scriere esuata: %s\n", room.name.c_str(), strerror(errno));
            ok = false;
        }
        buf.clear();
    };
    const auto feed = [&](const void* p, const size_t n) {
        buf.append(static_cast<const char*>(p), n);
        if (buf.size() >= (1 << 16)) flush_buf();
    };

    feed(SNAPSHOT_MAGIC, sizeof(SNAPSHOT_MAGIC));
    feed(&SNAPSHOT_VERSION, sizeof(SNAPSHOT_VERSION));
    feed(&num_records, sizeof(num_records));

    for (const auto& [key, rec] : room.keys) {
        const uint64_t klen = key.size();
        const uint64_t vlen = rec->value.size();
        feed(&klen, sizeof(klen));
        feed(key.data(), klen);
        feed(&vlen, sizeof(vlen));
        feed(rec->value.data(), vlen);
        feed(&rec->expire_at, sizeof(rec->expire_at));
        feed(&rec->timestamp_ms, sizeof(rec->timestamp_ms));
        feed(&rec->node_id, sizeof(rec->node_id));
        if (!ok) break;
    }
    flush_buf();

    if (!ok || !fsutil::sync_fd(fd)) {
        if (ok) fprintf(stderr, "Snapshot '%s': fdatasync a esuat: %s\n", room.name.c_str(), strerror(errno));
        ::close(fd);
        ::unlink(tmpname.c_str());
        return HibernateResult::Failure;
    }
    if (::close(fd) != 0) {
        fprintf(stderr, "Snapshot '%s': close esuat: %s\n", room.name.c_str(), strerror(errno));
        ::unlink(tmpname.c_str());
        return HibernateResult::Failure;
    }

    // rename atomic, dar VALIDAT: daca esueaza, inregistrarile raman in RAM
    // (vechiul cod le elibera indiferent de rezultatul redenumirii)
    if (::rename(tmpname.c_str(), filename.c_str()) != 0) {
        fprintf(stderr, "Snapshot '%s': rename esuat: %s\n", room.name.c_str(), strerror(errno));
        ::unlink(tmpname.c_str());
        return HibernateResult::Failure;
    }
    fsutil::sync_dir();

    *written = room.keys.size();
    destroy_all_records(room, pool);
    room.hibernated = true;
    return *written == 0 ? HibernateResult::Empty : HibernateResult::Written;
}
