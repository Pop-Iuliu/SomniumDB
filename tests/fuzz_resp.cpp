// SEC-10: fuzzing pentru parserele RESP, care citesc octeti direct de la straini.
// clang++ -std=c++20 -g -O1 -fsanitize=fuzzer,address,undefined tests/fuzz_resp.cpp -o fuzz_resp
// Sanitizerele prind citirile in afara bufferului; aici verificam contractul.
#include "../src/core/resp.h"

namespace {

using Parser = resp::Status (*)(const std::string&, size_t, size_t*, std::vector<std::string>*);

void check(const bool ok) {
    if (!ok) __builtin_trap();
}

// ca process_buffered: comenzi consecutive de la un offset, pana la primul
// rezultat care nu e Complete; fiecare pas e parsat de doua ori
void drain(const Parser parse, const std::string& buf) {
    std::vector<std::string> args, again;
    for (size_t pos = 0;;) {
        size_t end = 0, end_again = 0;
        const resp::Status st = parse(buf, pos, &end, &args);
        check(st == parse(buf, pos, &end_again, &again)); // determinist
        if (st != resp::Status::Complete) return;
        check(end > pos && end <= buf.size()); // avanseaza, fara sa iasa din buffer
        check(end == end_again && args == again);
        pos = end;
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, const size_t size) {
    const std::string buf(reinterpret_cast<const char*>(data), size);
    drain(resp::parse, buf);         // AOF si replicare
    drain(resp::parse_request, buf); // clientii: RESP sau inline
    return 0;
}
