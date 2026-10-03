// Verificarea ceasului HLC: header-only, ruleaza oriunde (fara server).
#undef NDEBUG
#include "../src/core/hlc.h"
#include <cassert>
#include <cstdio>

int main() {
    using hlc::Clock, hlc::normalize, hlc::physical_ms;
    const unsigned long long now = 1760000000000ull;

    // valorile vechi (ms) intra in formatul HLC; cele HLC raman neatinse
    assert(normalize(5) == 5ull << 16);
    assert(normalize(now << 16) == now << 16);
    assert(physical_ms(normalize(now)) == now);

    Clock clock;
    const auto a = clock.next(now, 0);
    assert(a == now << 16);
    const auto b = clock.next(now, 0); // aceeasi milisecunda: doar contorul creste
    assert(b == a + 1 && physical_ms(b) == now);

    // o scriere depaseste versiunea pe care o suprascrie, chiar venita de la un
    // nod cu ceasul 20s inainte (altfel peer-ul ar ignora-o: divergenta)
    const auto ahead = (now + 20000) << 16;
    const auto c = clock.next(now, ahead);
    assert(c > ahead && physical_ms(c) == now + 20000);

    // dupa ce am vazut o versiune, orice scriere ulterioara o depaseste
    clock.observe((now + 60000) << 16);
    assert(clock.next(now, 0) > (now + 60000) << 16);

    // monoton si cand ceasul fizic sare inapoi
    const auto before = clock.next(now, 0);
    assert(clock.next(now - 5000, 0) > before);

    puts("hlc_test: OK");
    return 0;
}
