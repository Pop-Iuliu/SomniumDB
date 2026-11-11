#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <string>

class CountMinSketch {
private:
    static constexpr int WIDTH = 4096; // trebuie sa fie putere a lui 2
    static constexpr int DEPTH = 4;
    uint16_t table[DEPTH][WIDTH] = {0};

    // Un singur hash per cheie; indicele randului i e h1 + i * h2 (Kirsch & Mitzenmacher,
    // "Less Hashing, Same Performance", ESA 2006), cu h2 impar ca sa acopere tot randul.
    uint16_t& cell(const uint64_t hash, const int row) {
        const uint32_t h1 = static_cast<uint32_t>(hash);
        const uint32_t h2 = static_cast<uint32_t>(hash >> 32) | 1;
        return table[row][(h1 + static_cast<uint32_t>(row) * h2) & (WIDTH - 1)];
    }

public:
    void record_access(const std::string& key) {
        const uint64_t hash = std::hash<std::string>{}(key);
        for (int i = 0; i < DEPTH; ++i) {
            if (uint16_t& count = cell(hash, i); count < 0xFFFF) ++count;
        }
    }

    uint16_t estimate_frequency(const std::string& key) {
        const uint64_t hash = std::hash<std::string>{}(key);
        uint16_t min_freq = 0xFFFF;
        for (int i = 0; i < DEPTH; ++i) min_freq = std::min(min_freq, cell(hash, i));
        return min_freq;
    }
};

