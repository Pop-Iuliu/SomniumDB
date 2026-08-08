cmake_minimum_required(VERSION 4.3)
project(Redis)

set(CMAKE_CXX_STANDARD 20)

add_executable(Redis main.cpp
        database.h
        database.cpp
        json.hpp
        watchdog.h
        watchdog.cpp
        pubsub.cpp
        pubsub.h
        pool_allocator.h
        bloom_filter.h
        metrics.h
        metrics.cpp
)
