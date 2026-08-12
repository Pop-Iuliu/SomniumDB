#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include "../../record.h"
#include "../../pool_allocator.h"
#include "../core/room.h"

class SnapshotManager {
public:
    static void wakeup_room(Room& room, PoolAllocator<Record, 1024>& pool);
    static void hibernate_room(Room& room, PoolAllocator<Record, 1024>& pool);
};

