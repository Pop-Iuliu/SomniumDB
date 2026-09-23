#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include "../../record.h"
#include "../../pool_allocator.h"
#include "../core/room.h"

// Rezultatul explicit al hibernarii:
//   Written - snapshot validat (write+fsync+close+rename) si camera eliberata din RAM
//   Empty   - camera nu avea continut; fisierul (gol) e scris, inregistrarile eliberate
//   Failure - persistenta a esuat: camera ramane ACTIVA si citibila, fara ack fals
enum class HibernateResult { Written, Empty, Failure };

// Format fisier snapshot (v2):
//   "RSNP" | uint32 version | size_t num_records
//   per record: klen | key | vlen | value | expire_at(i64) | timestamp_ms(u64) | node_id(u32)
// Fisierele v1 (fara magic) aveau doar: num_records, klen, key, vlen, value, expire_at.
class SnapshotManager {
public:
    // true doar pentru un fisier REGULAR de snapshot (stat ieftin, nu citeste datele)
    static bool has_snapshot(const std::string& room_name);

    // true = continut restaurat. Snapshot-ul e citit intr-o stare temporara;
    // inregistrarile vechi sunt eliberate doar dupa o incarcare completa si
    // valida. Un snapshot corupt NU se sterge: e redenumit .corrupt.<ts> ca
    // sa ramana pentru diagnostic.
    static bool wakeup_room(Room& room, PoolAllocator<Record, 1024>& pool);

    // written intoarce cate inregistrari au intrat in snapshot (0 pentru Empty)
    static HibernateResult hibernate_room(Room& room, PoolAllocator<Record, 1024>& pool, size_t* written);
};
