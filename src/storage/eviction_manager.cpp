#include "eviction_manager.h"
#include "../../metrics.h"
#include <random>
#include <ranges>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fstream>
#include <string_view>

void EvictionManager::evict_despised_keys(Room& room, PoolAllocator<Record, 1024>& pool) {
    while (room.keys.size() > MAX_KEYS_PER_ROOM) {
        constexpr int SAMPLE_SIZE = 5;
        std::string despised_key = "";
        uint16_t lowest_freq = 0xFFFF;

        size_t bucket_count = room.keys.bucket_count();
        if (bucket_count == 0) break;

        static std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<size_t> dist(0, bucket_count - 1);

        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            size_t random_bucket = dist(rng);
            auto it = room.keys.begin(random_bucket);

            while (it == room.keys.end(random_bucket)) {
                random_bucket = (random_bucket + 1) % bucket_count;
                it = room.keys.begin(random_bucket);
            }

            const std::string& candidate_key = it->first;
            uint16_t freq = cms.estimate_frequency(candidate_key);

            if (freq < lowest_freq) {
                lowest_freq = freq;
                despised_key = candidate_key;
            }
        }

        if (lowest_freq > 10) {
            for (const auto &k: room.keys | std::views::keys) {
                if (cms.estimate_frequency(k) <= 2) {
                    despised_key = k;
                    break;
                }
            }
        }

        if (!despised_key.empty() && room.keys.contains(despised_key)) {
            global_metrics.keys_evicted.fetch_add(1, std::memory_order_relaxed);
            global_metrics.keys_in_ram.fetch_sub(1, std::memory_order_relaxed);

            disk_shield.add(despised_key);
            if (std::ofstream cold_file("despised_keys.bin", std::ios::app | std::ios::binary); cold_file.is_open()) {
                size_t rlen = room.name.size();
                size_t klen = despised_key.size();
                size_t vlen = room.keys[despised_key]->value.size();

                cold_file.write(reinterpret_cast<const char*>(&rlen), sizeof(rlen));
                cold_file.write(room.name.data(), rlen);
                cold_file.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
                cold_file.write(despised_key.data(), klen);
                cold_file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
                cold_file.write(room.keys[despised_key]->value.data(), vlen);
                cold_file.close();
            }

            pool.deallocate(room.keys[despised_key]);
            room.keys.erase(despised_key);
        } else {
            break;
        }
    }
}

std::string EvictionManager::read_from_cold_storage(const std::string& room_name, const std::string& key) {
    const int fd = open("despised_keys.bin", O_RDONLY);
    if (fd < 0) return "";

    struct stat sb{};
    if (fstat(fd, &sb) == -1 || sb.st_size == 0) {
        close(fd);
        return "";
    }

    const auto mapped = static_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED) {
        close(fd);
        return "";
    }

    std::string found_value;
    size_t offset = 0;

    while (offset < static_cast<size_t>(sb.st_size)) {
        if (offset + sizeof(size_t) > sb.st_size) break;
        size_t rlen = *reinterpret_cast<size_t*>(mapped + offset);
        offset += sizeof(size_t);

        if (offset + rlen > sb.st_size) break;
        std::string_view current_room(mapped + offset, rlen);
        offset += rlen;

        if (offset + sizeof(size_t) > sb.st_size) break;
        size_t klen = *reinterpret_cast<size_t*>(mapped + offset);
        offset += sizeof(size_t);

        if (offset + klen > sb.st_size) break;
        std::string_view current_key(mapped + offset, klen);
        offset += klen;

        if (offset + sizeof(size_t) > sb.st_size) break;
        size_t vlen = *reinterpret_cast<size_t*>(mapped + offset);
        offset += sizeof(size_t);

        if (offset + vlen > sb.st_size) break;

        if (current_room == room_name && current_key == key) {
            found_value = std::string(mapped + offset, vlen);
        }
        offset += vlen;
    }

    munmap(mapped, sb.st_size);
    close(fd);

    return found_value;
}
