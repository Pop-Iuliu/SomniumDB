//
// Created by tiwerlol on 08.08.2026.
//
#pragma once

#include <vector>
#include <string>
#include <cstdint>

class BloomFilter {
private:
    std::vector<uint64_t> bits;
    size_t size_in_bits;
    int num_hashes;

    inline uint64_t hash64(const std::string& key) const {
        uint64_t hash = 14695981039346656037ULL;
        for (char c : key) {
            hash ^= static_cast<uint64_t>(c);
            hash *= 1099511628211ULL;
        }
        return hash;
    }

public:
    BloomFilter(size_t bits_count = 8388608, int k_hashes = 7) {
        size_in_bits = bits_count;
        bits.resize(size_in_bits / 64 + 1, 0);
        num_hashes = k_hashes;
    }

    void add(const std::string& key) {
        uint64_t h = hash64(key);
        uint32_t h1 = static_cast<uint32_t>(h >> 32);
        uint32_t h2 = static_cast<uint32_t>(h);

        for (int i = 0; i < num_hashes; i++) {
            uint32_t combined_hash = h1 + (i * h2);
            uint32_t bit_index = combined_hash % size_in_bits;

            bits[bit_index / 64] |= (1ULL << (bit_index % 64));
        }
    }

    bool possibly_exists(const std::string& key) const {
        uint64_t h = hash64(key);
        uint32_t h1 = static_cast<uint32_t>(h >> 32);
        uint32_t h2 = static_cast<uint32_t>(h);

        for (int i = 0; i < num_hashes; i++) {
            uint32_t combined_hash = h1 + (i * h2);
            uint32_t bit_index = combined_hash % size_in_bits;

            if ((bits[bit_index / 64] & (1ULL << (bit_index % 64))) == 0) {
                return false;
            }
        }
        return true;
    }
};