#include "snapshot_manager.h"
#include <fstream>
#include <cstdio>
#include <cstring>
#include <cstdint>

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
    std::ifstream file("room_" + room_name + ".bin", std::ios::binary);
    return file.is_open();
}

void SnapshotManager::wakeup_room(Room& room, PoolAllocator<Record, 1024>& pool) {
    const std::string filename = "room_" + room.name + ".bin";

    destroy_all_records(room, pool);

    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        return;
    }

    char first8[8] = {};
    if (!file.read(first8, sizeof(first8))) {
        file.close();
        std::remove(filename.c_str());
        return;
    }

    const bool is_v2 = std::memcmp(first8, SNAPSHOT_MAGIC, sizeof(SNAPSHOT_MAGIC)) == 0;
    size_t num_records = 0;

    if (is_v2) {
        // magic | version | num_records (primii 8 bytes au fost cititi deja)
        if (!file.read(reinterpret_cast<char*>(&num_records), sizeof(num_records))) {
            file.close();
            std::remove(filename.c_str());
            return;
        }
    } else {
        // format v1 (legacy): num_records direct
        std::memcpy(&num_records, first8, sizeof(num_records));
    }

    bool corrupt = false;
    for (size_t i = 0; i < num_records && !corrupt; ++i) {
        size_t klen = 0;
        size_t vlen = 0;
        if (!file.read(reinterpret_cast<char*>(&klen), sizeof(klen))) corrupt = true;

        std::string key;
        std::string val;
        if (!corrupt) {
            key.resize(klen);
            if (!file.read(key.data(), static_cast<std::streamsize>(klen))) corrupt = true;
        }
        if (!corrupt && !file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen))) corrupt = true;
        if (!corrupt) {
            val.resize(vlen);
            if (!file.read(val.data(), static_cast<std::streamsize>(vlen))) corrupt = true;
        }

        long long expire_at = 0;
        uint64_t timestamp_ms = 0;
        uint32_t node_id = 1;

        if (!corrupt && !file.read(reinterpret_cast<char*>(&expire_at), sizeof(expire_at))) corrupt = true;
        if (!corrupt && is_v2) {
            if (!file.read(reinterpret_cast<char*>(&timestamp_ms), sizeof(timestamp_ms))) corrupt = true;
            if (!corrupt && !file.read(reinterpret_cast<char*>(&node_id), sizeof(node_id))) corrupt = true;
        }

        if (corrupt) break;

        Record* rec = pool.construct(std::move(val), expire_at);
        rec->timestamp_ms = timestamp_ms;
        rec->node_id = node_id;
        room.keys[std::move(key)] = rec;
    }

    if (corrupt) {
        destroy_all_records(room, pool);
    }

    file.close();
    std::remove(filename.c_str());
    room.hibernated = false;
}

long long SnapshotManager::hibernate_room(Room& room, PoolAllocator<Record, 1024>& pool) {
    const std::string filename = "room_" + room.name + ".bin";
    const std::string tmpname = filename + ".tmp";

    std::ofstream file(tmpname, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        return -1;
    }

    file.write(SNAPSHOT_MAGIC, sizeof(SNAPSHOT_MAGIC));
    file.write(reinterpret_cast<const char*>(&SNAPSHOT_VERSION), sizeof(SNAPSHOT_VERSION));

    const size_t num_records = room.keys.size();
    file.write(reinterpret_cast<const char*>(&num_records), sizeof(num_records));

    for (const auto& [key, rec] : room.keys) {
        const size_t klen = key.size();
        const size_t vlen = rec->value.size();

        file.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
        file.write(key.data(), static_cast<std::streamsize>(klen));
        file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
        file.write(rec->value.data(), static_cast<std::streamsize>(vlen));
        file.write(reinterpret_cast<const char*>(&rec->expire_at), sizeof(rec->expire_at));
        file.write(reinterpret_cast<const char*>(&rec->timestamp_ms), sizeof(rec->timestamp_ms));
        file.write(reinterpret_cast<const char*>(&rec->node_id), sizeof(rec->node_id));
    }
    file.close();

    // rename atomic: wakeup nu poate citi niciodata un fisier partial scris
    std::rename(tmpname.c_str(), filename.c_str());

    const size_t written = room.keys.size();
    destroy_all_records(room, pool);
    room.hibernated = true;
    return static_cast<long long>(written);
}
