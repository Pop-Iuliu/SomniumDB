// SEC-23: fuzzing pentru Database::execute(), la care ajunge orice comanda autentificata.
// Build si rulare: jobul `fuzz` din .github/workflows/ci.yml.
// O singura baza de date pentru toata rularea, intr-un director temporar; fiecare
// input e o conexiune: comenzile ei, apoi deconectarea.
#include <cstdlib>
#include <strings.h>
#include <unistd.h>
#include "../src/core/database.h"
#include "../src/core/resp.h"

extern "C" int LLVMFuzzerInitialize(int*, char***) {
    char dir[] = "/tmp/somnium-fuzz-XXXXXX";
    if (!mkdtemp(dir) || chdir(dir) != 0) abort();
    setenv("SOMNIUM_AOF_SYNC", "no", 1);
    setenv("SOMNIUM_AOF_REWRITE_MIN_BYTES", "18446744073709551615", 1); // fara rescriere automata (fork)
    setenv("SOMNIUM_MAXMEMORY", "16777216", 1); // RSS marginit; restul ajunge in cold storage
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, const size_t size) {
    // creata dupa LLVMFuzzerInitialize: in directorul temporar, cu env-ul de mai sus
    static Database* const db = [] {
        auto* d = new Database;
        d->set_message_sink([](int, const std::string&) {}); // PUBLISH catre un abonat
        return d;
    }();

    constexpr int fd = 0;
    const std::string buf(reinterpret_cast<const char*>(data), size);
    std::vector<std::string> args;
    for (size_t pos = 0, end = 0; resp::parse_request(buf, pos, &end, &args) == resp::Status::Complete; pos = end) {
        if (args.empty() || strcasecmp(args[0].c_str(), "REWRITEAOF") == 0) continue; // forkeaza
        db->execute(fd, args);
    }
    db->cleanup_client(fd);
    return 0;
}
