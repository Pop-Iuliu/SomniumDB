#pragma once

#include <string>
#include <unordered_map>
#include "../../record.h"
#include "../../pool_allocator.h"
#include "../core/room.h"
#include "../utils/count_min_sketch.h"
#include "../../bloom_filter.h"

class EvictionManager {
private:
    CountMinSketch cms;
    BloomFilter disk_shield;
    const size_t MAX_KEYS_PER_ROOM = 1000000;

public:
    void record_access(const std::string& key) {
        cms.record_access(key);
    }

    bool possibly_on_disk(const std::string& key) {
        return disk_shield.possibly_exists(key);
    }

    void evict_despised_keys(Room& room, PoolAllocator<Record, 1024>& pool);

    std::string read_from_cold_storage(const std::string& room_name, const std::string& key);
};


