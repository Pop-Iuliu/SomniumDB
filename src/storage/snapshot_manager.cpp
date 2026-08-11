#include "snapshot_manager.h"
#include <fstream>
#include <cstdio>

void SnapshotManager::wakeup_room(Room& room, PoolAllocator<Record, 1024>& pool) {
    std::string filename = "room_" + room.name + ".bin";

    std::ifstream file(filename, std::ios::binary);
    if (file.is_open()) {
        try {
            room.keys.clear();

            size_t num_records = 0;
            if (file.read(reinterpret_cast<char*>(&num_records), sizeof(num_records))) {
                for (size_t i = 0; i < num_records; ++i) {
                    size_t klen = 0;
                    file.read(reinterpret_cast<char*>(&klen), sizeof(klen));
                    std::string key(klen, '\0');
                    file.read(key.data(), klen);

                    size_t vlen = 0;
                    file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
                    std::string val(vlen, '\0');
                    file.read(val.data(), vlen);

                    long long expire_at = 0;
                    file.read(reinterpret_cast<char*>(&expire_at), sizeof(expire_at));

                    room.keys[key] = pool.allocate(std::move(val), expire_at);
                }
            }
        } catch (const std::exception& e) {
            printf("-ERR eroare citire binara %s: %s\n", filename.c_str(), e.what());
            room.keys.clear();
        }

        file.close();
        std::remove(filename.c_str());
    } else {
        room.keys.clear();
    }
}

void SnapshotManager::hibernate_room(Room& room, PoolAllocator<Record, 1024>& pool) {
    std::string filename = "room_" + room.name + ".bin";
    std::ofstream file(filename, std::ios::binary);

    if (file.is_open()) {
        size_t num_records = room.keys.size();
        file.write(reinterpret_cast<const char*>(&num_records), sizeof(num_records));

        for (const auto& [key, record_ptr] : room.keys) {
            size_t klen = key.size();
            file.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
            file.write(key.data(), klen);

            size_t vlen = record_ptr->value.size();
            file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
            file.write(record_ptr->value.data(), vlen);

            file.write(reinterpret_cast<const char*>(&record_ptr->expire_at), sizeof(record_ptr->expire_at));

            pool.deallocate(record_ptr);
        }
        file.close();
    }
    room.keys.clear();
}
