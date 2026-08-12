#pragma once

#include <string>
#include <cstdint>

class CountMinSketch {
private:
    static constexpr int WIDTH = 4096; // trebuie sa fie putere a lui 2
    static constexpr int DEPTH = 4;
    uint16_t table[DEPTH][WIDTH] = {0};

    inline uint32_t fast_hash(const std::string& key, const uint32_t seed) {
        uint32_t h = seed;
        for (const char c : key) {
            h ^= static_cast<uint32_t>(c);
            h *= 0x5bd1e995;
            h ^= h >> 15;
        }
        return h;
    }

public:
    void record_access(const std::string& key) {
        for (int i = 0; i < DEPTH; ++i) {
            uint32_t h = fast_hash(key, (i + 1) * 0x9e3779b9) & (WIDTH - 1);
            if (table[i][h] < 0xFFFF) {
                table[i][h]++;
            }
        }
    }

    uint16_t estimate_frequency(const std::string& key) {
        uint16_t min_freq = 0xFFFF;
        for (int i = 0; i < DEPTH; ++i) {
            uint32_t h = fast_hash(key, (i + 1) * 0x9e3779b9) & (WIDTH - 1);
            if (table[i][h] < min_freq) {
                min_freq = table[i][h];
            }
        }
        return min_freq;
    }
};

