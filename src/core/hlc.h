#pragma once

#include <algorithm>
#include <cstdint>

// Hybrid Logical Clock (Kulkarni et al., "Logical Physical Clocks", OPODIS 2014)
// ca versiune CRDT: 48 de biti de milisecunde fizice | 16 biti de contor logic.
// Contorul absoarbe rafalele fara ca versiunile sa fuga inaintea timpului real.
namespace hlc {

// sub 2^47 e o valoare veche, in milisecunde (AOF, snapshot-uri, clienti care
// trimit ms): o mutam in formatul HLC. Idempotent pentru valorile HLC.
constexpr uint64_t kLegacyLimit = 1ull << 47;
inline uint64_t normalize(const uint64_t ts) { return ts < kLegacyLimit ? ts << 16 : ts; }
inline uint64_t physical_ms(const uint64_t ts) { return ts >> 16; }

// Folosit doar de thread-ul de comenzi (si de replay, inainte de pornire).
class Clock {
    uint64_t last_ = 0;

public:
    // versiunea unei scrieri locale: strict peste ceasul local si peste
    // versiunea pe care o suprascrie, deci LWW o accepta pe orice nod
    uint64_t next(const long long now_ms, const uint64_t overwritten) {
        last_ = std::max({static_cast<uint64_t>(now_ms) << 16, last_ + 1, overwritten + 1});
        return last_;
    }

    // o versiune vazuta (merge primit, replay): scrierile ulterioare o depasesc
    void observe(const uint64_t ts) { last_ = std::max(last_, ts); }
};

} // namespace hlc
