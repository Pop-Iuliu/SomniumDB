#pragma once

#include <string>
#include "../../record.h"
#include "../../pool_allocator.h"
#include "../core/room.h"

// Format fisier snapshot (v2):
//   "RSNP" | uint32 version | size_t num_records
//   per record: klen | key | vlen | value | expire_at(i64) | timestamp_ms(u64) | node_id(u32)
// Fisierele v1 (fara magic) aveau doar: num_records, klen, key, vlen, value, expire_at.
class SnapshotManager {
public:
    // true daca exista snapshot pe disk pentru camera (stat ieftin, nu citeste datele)
    static bool has_snapshot(const std::string& room_name);

    static void wakeup_room(Room& room, PoolAllocator<Record, 1024>& pool);
    static long long hibernate_room(Room& room, PoolAllocator<Record, 1024>& pool);
};
