#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include "../../record.h"
#include "../../pool_allocator.h"
#include "../core/room.h"
#include "../utils/count_min_sketch.h"

// Cold storage: inregistrarile evict-uite (valoare, expirare absoluta,
// versiune CRDT) stau in fisierul append-only cold.bin, iar un index in RAM
// (camera, cheie) -> ultima inregistrare e sursa de adevar. O cheie e fie in
// RAM, fie in index, niciodata in ambele: reincarcarea o scoate din index,
// deci un DEL sau o expirare in RAM nu pot invia o valoare veche de pe disc.
//
// Fisierul traieste cat procesul: la pornire AOF-ul reda toata starea in RAM,
// deci il trunchiem. De aceea nu sunt necesari markeri de stergere pe disc
// si nici reconstruirea indexului. Folosit doar de thread-ul de comenzi.
class EvictionManager {
public:
    EvictionManager();
    ~EvictionManager();

    void record_access(const std::string& key) { cms.record_access(key); }

    // muta cheile cele mai putin folosite in cold storage pana camera respecta
    // limita (SOMNIUM_MAX_KEYS); o scriere esuata lasa inregistrarea in RAM
    void evict_despised_keys(Room& room, PoolAllocator<Record, 1024>& pool);

    // scoate cheia din cold storage (apelantul o muta in RAM);
    // nullopt = absenta, distinct de o valoare goala
    std::optional<Record> take(const std::string& room_name, const std::string& key);

    // rescrie doar inregistrarile vii si neexpirate; fisierul si indexul nou se
    // publica impreuna, doar daca toate scrierile au reusit. Intoarce octetii
    // recuperati sau -1 la esec (fisierul si indexul vechi raman in uz).
    long long compact(long long now_ms);

    // fiecare cheie din cold storage, cu inregistrarea ei (rescrierea AOF);
    // false = o citire a esuat
    bool for_each(const std::function<void(const std::string& room, const std::string& key, const Record&)>& fn) const;

private:
    struct ColdRef {
        uint64_t off;
        uint64_t len;
    };

    CountMinSketch cms;
    std::unordered_map<std::string, std::unordered_map<std::string, ColdRef>> index;
    int fd = -1;
    uint64_t file_bytes = 0;
    uint64_t obsolete_bytes = 0;
    uint64_t max_keys_per_room = 1000000;

    void publish_metrics() const;
};
