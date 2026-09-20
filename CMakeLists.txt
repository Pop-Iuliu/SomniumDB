cmake_minimum_required(VERSION 4.3)
project(Redis)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE Release)

add_executable(Redis main.cpp
        src/core/database.h
        src/core/database.cpp
        json.hpp
        watchdog.h
        watchdog.cpp
        pubsub.cpp
        pubsub.h
        pool_allocator.h
        bloom_filter.h
        metrics.h
        metrics.cpp
        src/storage/aof_manager.h
        src/storage/aof_manager.cpp
        src/storage/snapshot_manager.h
        src/storage/snapshot_manager.cpp
        src/storage/eviction_manager.h
        src/storage/eviction_manager.cpp
        src/utils/count_min_sketch.h
)

target_link_libraries(Redis uring)
