#pragma once

#include <string>
#include <cstdint>
#include <functional>

class CountMinSketch {
private:
    static constexpr int WIDTH = 4096; // trebuie sa fie putere a lui 2
    static constexpr int DEPTH = 4;
    uint16_t table[DEPTH][WIDTH] = {0};
    static_assert(DEPTH * 16 <= 64 && WIDTH <= (1 << 16));

    // un singur hash pe 64 de biti (std::hash, 8 octeti pe pas) in loc de DEPTH
    // treceri octet cu octet; fiecare rand isi ia 16 biti disjuncti din el
    static uint64_t key_hash(const std::string& key) { return std::hash<std::string>{}(key); }
    static uint32_t slot(const uint64_t h, const int row) { return (h >> (16 * row)) & (WIDTH - 1); }

public:
    void record_access(const std::string& key) {
        const uint64_t h = key_hash(key);
        for (int i = 0; i < DEPTH; ++i) {
            uint16_t& counter = table[i][slot(h, i)];
            if (counter < 0xFFFF) {
                counter++;
            }
        }
    }

    uint16_t estimate_frequency(const std::string& key) {
        const uint64_t h = key_hash(key);
        uint16_t min_freq = 0xFFFF;
        for (int i = 0; i < DEPTH; ++i) {
            if (table[i][slot(h, i)] < min_freq) {
                min_freq = table[i][slot(h, i)];
            }
        }
        return min_freq;
    }
};

