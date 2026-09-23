#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

// O mutatie persistata: comanda tintita intr-o camera plus starea REZULTATA.
// Meta-informatia (expirare absoluta, timestamp CRDT, nod) calatoreste cu
// inregistrarea, ca replay-ul sa reproduca mutatia originala si nu sa o
// reexecute (altfel SET-urile redate si-ar regenera timestampuri noi si ar
// rasturna ordinea de convergenta CRDT fata de nodurile remote).
struct AofRecord {
    std::string room;
    std::vector<std::string> args; // [CMD, ...argumente]
    long long expire_at = 0;
    uint64_t timestamp_ms = 0;
    uint32_t node_id = 1;
};

// Format AOF v3 ("SOMNIUM-AOF", versiune 3):
//   *2\r\n$10\r\nSOMNIUM-AOF\r\n$1\r\n3\r\n   (header, o singura data, la inceput)
//   [room, CMD, args..., "SOMNIUM-META", expire_at, timestamp_ms, node_id]
// Numele camerei e intotdeauna primul token: o camera numita "SET" nu poate
// fi confundata cu un marker de format (ghicirea per-inregistrare din vechiul
// cod era ambigua exact in cazul asta).
// Formate vechi, recunoscute la pornire si migrate explicit catre v3:
//   v1: [CMD, args...]      (fara camera -> se redate in "default")
//   v2: [room, CMD, args...] (fara meta; vechiul format cu prefix de camera)
class AOFManager {
public:
    // Politica de durabilitate (SOMNIUM_AOF_SYNC):
    //   always   - fdatasync dupa fiecare comanda (durabilitate maxima, lent)
    //   everysec - fdatasync la cel mult o secunda (default, ca Redis)
    //   no       - niciodata explicit; doar page cache (pierdere la pana de curent)
    enum class SyncPolicy { Always, Everysec, Off };

    AOFManager();
    ~AOFManager();

    // Reda istoricul prin callback, inregistrare cu inregistrare.
    // Coada incompleta (crash in mijlocul unei comenzi) e trunchiata la
    // ultimul offset valid, pastrand exact istoricul complet. Coruperea din
    // mijloc NU se trunchiaza: fisierul e redenumit .corrupt.<ts> pentru
    // diagnostic si se porneste un AOF nou; starea redata se reserializeaza
    // in v3 (needs_rewrite()).
    void recover(const std::function<void(const AofRecord&)>& replay);

    // Deschide fd-ul de append; scrie headerul v3 doar daca fisierul e nou.
    bool open_file();

    // Append al mutatiei rezultate. false = scrierea a esuat: starea din RAM
    // ramane, dar persistenta trebuie suspectata (niciodata ack fals).
    bool append(const std::string& room_name, const std::vector<std::string>& args,
                long long expire_at, uint64_t timestamp_ms, uint32_t node_id);

    // Migrare explicita v1/v2 -> v3: starea actuala (redata deja in RAM) se
    // serialaza intr-un fisier temporar si se substituie atomic celui vechi.
    // Esuarea in orice punct lasa fisierul vechi neatins.
    bool start_rewrite();
    bool append_rewrite(const AofRecord& rec);
    bool commit_rewrite();
    void abort_rewrite();
    // true daca fisierul redat nu era v3 (sau era corupt): necesita reserializare
    bool needs_rewrite() const { return needs_rewrite_; }

    // Sincronizare granulata pentru politica everysec; apelata din watchdog
    // (thread-ul de comenzi nu blocheaza niciodata pe fdatasync).
    void sync_if_due();

    bool healthy() const { return healthy_; }
    SyncPolicy policy() const { return sync_policy_; }
    const char* policy_name() const;

private:
    enum class ParseStatus { Complete, NeedMore, Malformed };

    int fd_ = -1;          // appendonly.aof
    int rewrite_fd_ = -1;  // appendonly.aof.tmp, doar in timpul migrarii
    SyncPolicy sync_policy_;
    std::mutex mutex_;
    std::atomic<bool> dirty_{false};
    std::atomic<long long> last_sync_ms_{0};
    bool healthy_ = true;
    bool needs_rewrite_ = false;
    bool replaying_ = true;

    static std::string encode_record(const AofRecord& rec);
    static ParseStatus parse_resp_array(const std::string& buf, size_t pos,
                                        size_t* rec_end, std::vector<std::string>* tokens);
    static long long now_ms();
    bool sync_locked(int fd);
    void sync_due_locked();
    void abort_locked(); // elibereaza rewrite_fd_ + tmp; presupune mutex_ prins
};
