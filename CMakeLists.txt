cmake_minimum_required(VERSION 3.20)
project(Redis)

set(CMAKE_CXX_STANDARD 20)
# default Release, dar respectam tipul cerut (CI: Debug + sanitizere)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# SEC-11: hardening explicit, oriunde e construit binarul (nu doar unde
# compilatorul il activeaza implicit)
add_compile_options(-fstack-protector-strong)
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2)
endif()
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_link_options(-pie -Wl,-z,relro,-z,now)

add_executable(Redis main.cpp
        src/core/database.h
        src/core/database.cpp
        src/core/resp.h
        src/core/hlc.h
        json.hpp
        watchdog.h
        watchdog.cpp
        pubsub.cpp
        pubsub.h
        pool_allocator.h
        metrics.h
        metrics.cpp
        src/storage/aof_manager.h
        src/storage/aof_manager.cpp
        src/storage/fs_util.h
        src/storage/snapshot_manager.h
        src/storage/snapshot_manager.cpp
        src/storage/eviction_manager.h
        src/storage/eviction_manager.cpp
        src/storage/replicator.h
        src/storage/replicator.cpp
        src/utils/count_min_sketch.h
)

target_link_libraries(Redis uring)

# verificarea parserului RESP: header-only, fara server
add_executable(resp_test tests/resp_test.cpp)
add_executable(hlc_test tests/hlc_test.cpp)

enable_testing()
add_test(NAME somnium_suite
         COMMAND bash ${CMAKE_SOURCE_DIR}/tests/run_all.sh $<TARGET_FILE:Redis>)
set_tests_properties(somnium_suite PROPERTIES TIMEOUT 900)
