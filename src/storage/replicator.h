#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <string>
#include <thread>
#include <vector>

// S10 "koreish": replicare CRDT in caravane. Thread-uri proprii (I/O-ul de retea
// nu are voie sa intarzie fsync-ul AOF din watchdog) citeste AOF-ul local de la
// un offset salvat per peer si trimite inregistrarile scrise pe acest nod ca un
// singur batch pipeline de CRDTMERGE, cu ROOM unde camera se schimba.
//
// CRDTMERGE e comutativ si idempotent, deci livrarea at-least-once ajunge: fara
// protocol de ordonare. Inregistrarile primite de la alti noduri poarta nodul
// lor in meta si nu se mai trimit mai departe (nimic nu se intoarce in ecou).
// Offsetul avanseaza doar pana la ultima inregistrare confirmata si se salveaza
// impreuna cu inode-ul AOF-ului: o rescriere (S8) creeaza alt fisier, iar
// atunci retrimitem de la inceput.
//
// Se replica scrierile (SET, MSET, CRDTMERGE; EXPIRE/PERSIST se logheaza ca SET
// versionat) si stergerile (DEL, CRDTDEL ca CRDTDEL). Dupa un batch care a ajuns
// la capatul AOF-ului pleaca REPLFRONTIER node F: toate scrierile locale <= F
// sunt livrate, deci peer-ul poate uita tombstone-urile mai vechi (S13).
//
// Indulgent (Guerraoui, PODC 2000): siguranta (convergenta) nu depinde deloc de
// detectia defectelor, fiindca merge-urile sunt idempotente. O suspiciune falsa
// costa doar intarziere, deci fiecare peer are thread-ul lui (un peer inaccesibil
// nu le blocheaza pe celelalte), connect marginit in timp, backoff exponential la
// esec si un heartbeat (REPLFRONTIER) la fiecare caravana, din care se calculeaza phi.
class Replicator {
public:
    // citeste SOMNIUM_NODE_ID si SOMNIUM_PEERS ("host:port,host:port"); frontier e
    // versiunea pana la care toate scrierile locale sunt deja in AOF (Database)
    explicit Replicator(const std::atomic<uint64_t>& frontier);
    ~Replicator();

    void start(); // nu face nimic fara peers

private:
    struct Peer {
        std::string host;
        std::string port;
        std::string state_file; // "inode offset", ca restartul sa reia de unde a ramas
        int fd = -1;
        uint64_t inode = 0;
        uint64_t offset = 0;
        std::string room; // camera selectata pe conexiune ("" = necunoscuta)
        std::deque<double> intervals; // ultimele intervale intre heartbeat-uri (ms)
        long long last_beat_ms = 0;
        uint64_t lag = 0; // octeti din AOF inca neconfirmati
    };

    uint32_t node_id_ = 1;
    const std::atomic<uint64_t>& frontier_;
    std::vector<Peer> peers_;
    std::atomic<bool> running_{false};
    std::vector<std::thread> workers_;

    bool ship(Peer& peer); // true = peer-ul a raspuns (heartbeat)
    void beat(Peer& peer);
    static void publish(const Peer& peer);
    bool deliver(Peer& peer, const std::string& out, size_t expected, size_t* acked);
    static void save(const Peer& peer);
};
