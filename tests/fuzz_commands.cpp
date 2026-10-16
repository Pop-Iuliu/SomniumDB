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
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, const size_t size) {
    // creata dupa LLVMFuzzerInitialize, deci in directorul temporar
    static Database* const db = [] {
        Config cfg;
        cfg.aof_sync = AofSync::Off;
        cfg.aof_rewrite_min_bytes = UINT64_MAX; // fara rescriere automata (fork)
        cfg.maxmemory = 16 << 20;               // RSS marginit; restul ajunge in cold storage
        auto* d = new Database(cfg);
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
