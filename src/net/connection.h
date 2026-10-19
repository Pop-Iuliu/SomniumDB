#pragma once

// REF-5: conexiunea unui client, de la socket pana la Database: bufferele si
// capurile lor (SEC-17, SEC-19, SEC-22), TLS (SEC-16), parsarea si poarta AUTH
// (SEC-2). Nu atinge ring-ul io_uring si nici registrul clientilor: intoarce ce
// trebuie facut (inchis, armat, pompat), iar bucla din main.cpp face.

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <openssl/ssl.h>
#include "../config.h"

class Database;

namespace net {

// o singura data, inainte de primul client: limitele, parola si contextul TLS
void init(const Config& cfg, SSL_CTX* tls);

struct Client {
    int fd;
    std::string in_buf;
    std::string out_buf;
    size_t out_off = 0; // bytes deja plecati din out_buf
    bool closed = false;
    bool read_armed = false; // starea buclei: un poll e in ring
    bool write_armed = false;
    bool reply_due = false;  // starea buclei: raspunsurile asteapta commit-ul de grup AOF
    bool resp3 = false; // protocolul negociat prin HELLO
    bool authenticated;
    size_t held = 0; // cat din totalul bufferelor (SEC-22) e al acestui client
    std::unique_ptr<SSL, decltype(&SSL_free)> ssl; // SEC-16; nullptr = text clar
    const std::chrono::steady_clock::time_point connected = std::chrono::steady_clock::now();

    explicit Client(int f);
    size_t pending_out() const { return out_buf.size() - out_off; }
    size_t input_cap() const;
    size_t output_cap() const;
};

// SEC-22: aduce totalul la zi dupa orice schimbare a bufferelor clientului
// (si dupa inchidere: un client inchis nu mai tine nimic)
void track(Client& c);

// citeste tot ce e disponibil, prin TLS daca e cazul; false = clientul trebuie inchis
bool drain_read(Client& c);

// Complete = tot a plecat; Pending = socketul e plin, reluam pe POLLOUT; Dead = eroare
enum class FlushResult { Complete, Pending, Dead };
FlushResult flush_output(Client& c);

// un raspuns in coada (cifrat cu TLS daca e cazul); false = un cap depasit, clientul trebuie inchis
bool enqueue_output(Client& c, const std::string& data);

// ruleaza comenzile complete din in_buf, pana la buget, si pune raspunsurile in out_buf.
// Nu trimite nimic: bucla trimite raspunsurile abia dupa write-ul AOF care le acopera
// (Database::flush_aof, commit de grup in main.cpp). Done: bucla trimite si rearmeaza
// citirea; More: la fel, iar bugetul a lasat input complet neprocesat; Drop: clientul
// trebuie inchis.
enum class Step { Drop, Done, More };
Step process_buffered(Client& c, Database& db);

} // namespace net
