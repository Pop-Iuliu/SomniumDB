#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
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
    //   always   - fdatasync dupa fiecare comanda, inainte de ack (durabilitate maxima, lent)
    //   everysec - fdatasync pe un thread dedicat, la intervalul tinta
    //              SOMNIUM_AOF_SYNC_INTERVAL_MS (implicit 1000). Nu e o fereastra
    //              stricta de o secunda: daca fdatasync depaseste intervalul, pierderea
    //              posibila creste si overrun-ul apare in metrici
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

    // Rescriere (si migrare v1/v2 -> v3): starea curenta se serializeaza intr-un
    // fisier temporar si se substituie atomic celui vechi. Esuarea in orice punct
    // lasa fisierul vechi in uz.
    bool start_rewrite();
    bool append_rewrite(const AofRecord& rec);
    bool commit_rewrite();
    void abort_rewrite();

    // rescrierea in fundal (S14): copilul fork-ului scrie direct in fd-ul temporar
    // (fara mutexul de aici, pe care il putea tine alt thread la fork), iar
    // parintele adauga apoi coada fisierului curent de la offsetul fork-ului
    int rewrite_fd() const { return rewrite_fd_; }
    uint64_t size() const { return size_.load(std::memory_order_relaxed); }
    bool append_tail(uint64_t from);
    static std::string encode_record(const AofRecord& rec);
    // true daca fisierul redat nu era v3 (sau era corupt): necesita reserializare
    bool needs_rewrite() const { return needs_rewrite_; }

    // rescriere automata: AOF-ul s-a dublat fata de dimensiunea de dupa ultima
    // rescriere si a trecut de SOMNIUM_AOF_REWRITE_MIN_BYTES (implicit 64 MB).
    // Fiecare incercare reseteaza baza, deci un esec nu se reia la fiecare comanda.
    bool rewrite_due() const {
        const uint64_t size = size_.load(std::memory_order_relaxed);
        return size >= rewrite_min_ && size >= 2 * base_size_.load(std::memory_order_relaxed);
    }

    bool healthy() const { return healthy_; }
    SyncPolicy policy() const { return sync_policy_; }
    const char* policy_name() const;

private:
    int fd_ = -1;          // appendonly.aof
    int rewrite_fd_ = -1;  // appendonly.aof.tmp, doar in timpul migrarii
    SyncPolicy sync_policy_;
    std::mutex mutex_;
    std::condition_variable sync_cv_;
    std::thread sync_thread_;
    bool sync_stop_ = false;
    // generatia creste la fiecare inlocuire a fd-ului. O sincronizare pornita pe
    // generatia veche nu are voie sa marcheze generatia noua ca durabila.
    uint64_t generation_ = 0;
    uint64_t synced_ = 0; // prefixul durabil al generatiei curente
    long long last_sync_epoch_ms_ = 0;
    long long sync_interval_ms_ = 1000;
    int sync_delay_ms_ = 0; // SOMNIUM_AOF_SYNC_DELAY_MS: intarziere de test in fdatasync
    std::chrono::steady_clock::time_point next_attempt_{};
    std::atomic<bool> healthy_{true}; // citit fara mutex de thread-ul de comenzi
    bool needs_rewrite_ = false;
    std::atomic<uint64_t> size_{0};      // octetii fisierului curent
    std::atomic<uint64_t> base_size_{0}; // dimensiunea dupa ultima rescriere (sau la pornire)
    uint64_t last_tail_ = 0;             // coada copiata peste instantaneu, la ultima rescriere
    uint64_t rewrite_min_ = 64ull << 20;

    void publish_sizes() const;
    void publish_sync_locked() const; // presupune mutex_ prins
    bool replaying_ = true;

    static long long now_ms();
    bool sync_fd_delayed(int fd) const;
    bool sync_locked(int fd); // fdatasync sub mutex; doar always si pornirea
    void note_append_wait(std::chrono::steady_clock::time_point start) const;
    void bump_generation_locked();
    void sync_worker();
    void stop_sync_worker();
    void abort_locked(); // elibereaza rewrite_fd_ + tmp; presupune mutex_ prins
};
