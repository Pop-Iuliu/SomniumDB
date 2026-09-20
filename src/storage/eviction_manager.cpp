#include "eviction_manager.h"
#include "../../metrics.h"
#include <random>
#include <ranges>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <fstream>
#include <string_view>

namespace {
    // format cold storage: rlen | room | klen | key | vlen | value (append-only)
    template <typename Fn>
    void for_each_cold_record(const char* mapped, size_t size, Fn&& fn) {
        size_t offset = 0;
        auto read_size = [&](size_t& out) {
            if (offset + sizeof(size_t) > size) return false;
            std::memcpy(&out, mapped + offset, sizeof(size_t));
            offset += sizeof(size_t);
            return true;
        };

        while (offset < size) {
            size_t rlen = 0;
            if (!read_size(rlen) || offset + rlen + sizeof(size_t) > size) break;
            const std::string_view room(mapped + offset, rlen);
            offset += rlen;

            size_t klen = 0;
            if (!read_size(klen) || offset + klen + sizeof(size_t) > size) break;
            const std::string_view key(mapped + offset, klen);
            offset += klen;

            size_t vlen = 0;
            if (!read_size(vlen) || offset + vlen > size) break;
            const std::string_view value(mapped + offset, vlen);
            offset += vlen;

            if (fn(room, key, value)) break;
        }
    }
} // namespace

EvictionManager::EvictionManager() {
    reload_disk_shield();
}

void EvictionManager::reload_disk_shield() {
    const int fd = open("despised_keys.bin", O_RDONLY);
    if (fd < 0) return;

    struct stat sb{};
    if (fstat(fd, &sb) == -1 || sb.st_size == 0) {
        close(fd);
        return;
    }

    const auto mapped = static_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED) {
        close(fd);
        return;
    }

    size_t restored = 0;
    for_each_cold_record(mapped, static_cast<size_t>(sb.st_size), [&](std::string_view, std::string_view key, std::string_view) {
        disk_shield.add(std::string(key));
        restored++;
        return false;
    });

    munmap(mapped, sb.st_size);
    close(fd);

    if (restored > 0) {
        printf("Disk Shield reconstruit: %zu chei evict-uite cunoscute.\n", restored);
    }
}

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
                cold_file.write(room.name.data(), static_cast<std::streamsize>(rlen));
                cold_file.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
                cold_file.write(despised_key.data(), static_cast<std::streamsize>(klen));
                cold_file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
                cold_file.write(room.keys[despised_key]->value.data(), static_cast<std::streamsize>(vlen));
            }

            pool.destroy(room.keys[despised_key]);
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

    // ultima aparitie castiga: cheile re-evict-uite produc duplicate, cel mai
    // recent append este si cea mai noua valoare
    std::string found_value;
    for_each_cold_record(mapped, static_cast<size_t>(sb.st_size), [&](std::string_view room, std::string_view k, std::string_view v) {
        if (room == room_name && k == key) {
            found_value.assign(v.begin(), v.end());
        }
        return false;
    });

    munmap(mapped, sb.st_size);
    close(fd);

    return found_value;
}
