#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <optional>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <csignal>
#include <cerrno>
#include <chrono>
#include <liburing.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "src/server/metrics.h"
#include "src/core/database.h"
#include "src/config.h"
#include "src/net/auth.h"
#include "src/net/connection.h"
#include "src/storage/replicator.h"
#include "src/server/watchdog.h"

using namespace std;
using net::Client;

#define URING_ENTRIES 4096
static constexpr auto AUTH_TIMEOUT = chrono::seconds(10); // SEC-18: cat poate tine un strain un loc

static optional<Database> db; // REF-1: construita in main(), dupa verificarile de pornire
static Config cfg; // REF-2: citita o singura data, la pornire (load_config)
static SSL_CTX* g_tls = nullptr; // SEC-16: SOMNIUM_TLS_CERT + SOMNIUM_TLS_KEY; nullptr = text clar

// S3 "naigie": calutul de munca care nu se opreste pentru niciun client.
// Fiecare client are o coada de output ordonata; scrierile partiale se reiau
// pe evenimente io_uring POLLOUT, niciodata prin asteptare blocanta in timpul
// unei comenzi. Clientii care nu urmeaza citirea sunt deconectati la un cap.

// un PollTicket zboara cu fiecare poll armat in ring; shared_ptr tine clientul
// viu pana la CQE, chiar daca intre timp a fost deconectat
struct PollTicket {
    shared_ptr<Client> client;
    bool for_write;
};

static PollTicket eventfd_ticket{nullptr, false};
static PollTicket listener_ticket{nullptr, false};

static unordered_map<int, shared_ptr<Client>> clients;
static vector<PollTicket*> arm_backlog;
static int g_eventfd = -1;
static int g_server_fd = -1;

static void set_nonblocking(const int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void schedule_close(const shared_ptr<Client>& c) {
    if (c->closed) return;
    c->closed = true;
    net::track(*c);
    if (cfg.trace_fd == -2 || cfg.trace_fd == c->fd)
        printf("[TRACE] fd %d CLOSE\n", c->fd);
    db->cleanup_client(c->fd);
    // un poll armat in ring tine socketul (si clientul, cu bufferele lui) in viata
    // dupa close; shutdown il trezeste acum si inchide conexiunea efectiv
    ::shutdown(c->fd, SHUT_RDWR);
    ::close(c->fd);
    clients.erase(c->fd);
    // ticketele in zbor tin shared_ptr; CQE-urile viitoare vad closed si nu fac nimic
}

static void arm_client(const shared_ptr<Client>& c, const bool for_write) {
    if (c->closed) return;
    if (for_write && c->write_armed) return; // deja armat: doar bufferam
    if (!for_write && c->read_armed) return;
    // flagul se reporneste la CQE (sau la inchiderea clientului)
    if (for_write) c->write_armed = true;
    else c->read_armed = true;
    if (cfg.trace_fd == -2 || cfg.trace_fd == c->fd)
        printf("[TRACE] fd %d arm %s (backlog %zu)\n", c->fd, for_write ? "POLLOUT" : "POLLIN", arm_backlog.size());
    arm_backlog.push_back(new PollTicket{c, for_write});
}

// inainte de submit: transformam armarile din backlog in SQE-uri.
// ticketele plecate in ring sunt eliberate la procesarea CQE-ului lor
static void submit_backlog(io_uring* ring) {
    size_t kept = 0;
    for (size_t i = 0; i < arm_backlog.size(); ++i) {
        PollTicket* t = arm_backlog[i];
        if (t->client->closed) {
            delete t; // inchis intre timp; fara CQE, eliberam aici
            continue;
        }
        io_uring_sqe* sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            arm_backlog[kept++] = t; // ring plin: incercam la ciclul urmator
            continue;
        }
        io_uring_prep_poll_add(sqe, t->client->fd, t->for_write ? POLLOUT : POLLIN);
        io_uring_sqe_set_data(sqe, t);
        // ownership mutat in ring: ticketul e eliberat la procesarea CQE-ului
    }
    arm_backlog.resize(kept);
}

static uint64_t eventfd_drain_buffer; // buffer stabil pentru citirea eventfd-ului

// listenerul NU mai foloseste poll: accept direct in ring (op blocanta,
// CQE doar cand exista conexiune). Pollul pe socketul de listen s-a dovedit
// fragil la kernel 6.8 in setupul asta (CQE pierdut la pornire).
static void arm_listener(io_uring* ring) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        if (cfg.trace_fd == -2) printf("[TRACE] arm_listener ESUAT (sqe null)\n");
        return;
    }
    io_uring_prep_accept(sqe, g_server_fd, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
    io_uring_sqe_set_data(sqe, &listener_ticket);
    const int submitted = io_uring_submit(ring);
    if (cfg.trace_fd == -2) printf("[TRACE] arm_listener submit=%d\n", submitted);
}

static void arm_eventfd(io_uring* ring) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) return;
    // read blocant in ring: CQE doar cand contorul eventfd e nenul
    io_uring_prep_read(sqe, g_eventfd, &eventfd_drain_buffer, sizeof(eventfd_drain_buffer), 0);
    io_uring_sqe_set_data(sqe, &eventfd_ticket);
    const int submitted = io_uring_submit(ring);
    if (cfg.trace_fd == -2) printf("[TRACE] arm_eventfd submit=%d\n", submitted);
}

static void deliver_pubsub(const int fd, const string& msg) {
    const auto it = clients.find(fd);
    if (it == clients.end() || it->second->closed) return;
    if (it->second->resp3) {
        // RESP3: acelasi mesaj, trimis ca push ('>' in loc de '*'), ca sa nu se
        // confunde cu raspunsul unei comenzi trimise de abonat intre timp
        string push = msg;
        push[0] = '>';
        if (!net::enqueue_output(*it->second, push)) return schedule_close(it->second);
    } else if (!net::enqueue_output(*it->second, msg)) {
        return schedule_close(it->second);
    }

    // incercam trimisul direct; daca fd-ul e plin, POLLOUT il ia mai tarziu
    const auto r = net::flush_output(*it->second);
    if (r == net::FlushResult::Pending) arm_client(it->second, true);
    else if (r == net::FlushResult::Dead) schedule_close(it->second);
}

static void queue_work() {
    uint64_t one = 1;
    if (write(g_eventfd, &one, sizeof(one)) < 0 && errno != EAGAIN) {
        cerr << "eventfd write esuat: " << strerror(errno) << "\n";
    }
}

// ruleaza inputul clientului si face ce a cerut conexiunea (REF-5: inchiderea si
// armarea raman aici); true = bugetul a lasat input complet neprocesat
static bool serve(const shared_ptr<Client>& c) {
    const net::Step step = net::process_buffered(*c, *db);
    if (step == net::Step::Drop) {
        schedule_close(c);
        return false;
    }
    if (c->pending_out() > 0) arm_client(c, true); // socketul e plin: restul pleaca pe POLLOUT
    arm_client(c, false);                           // re-arm read
    return step == net::Step::More;
}

// SEC-16: SOMNIUM_TLS_CERT si SOMNIUM_TLS_KEY pornesc TLS pentru clienti si pentru replicare
static bool load_tls() {
    const auto& cert = cfg.tls_cert;
    const auto& key = cfg.tls_key;
    if (!cert && !key) return true;
    g_tls = SSL_CTX_new(TLS_method());
    if (cert && key && g_tls && SSL_CTX_set_min_proto_version(g_tls, TLS1_2_VERSION) == 1 &&
        SSL_CTX_use_certificate_chain_file(g_tls, cert->c_str()) == 1 &&
        SSL_CTX_use_PrivateKey_file(g_tls, key->c_str(), SSL_FILETYPE_PEM) == 1 && SSL_CTX_check_private_key(g_tls) == 1) {
        return true;
    }
    cerr << "TLS: SOMNIUM_TLS_CERT si SOMNIUM_TLS_KEY trebuie sa fie impreuna un certificat PEM si cheia lui\n";
    ERR_print_errors_fp(stderr);
    return false;
}

// SEC-18: o conexiune care nu se autentifica la timp elibereaza locul (fara
// parola toti clientii sunt autentificati, deci nu se inchide nimic)
static void close_unauthenticated(const chrono::steady_clock::time_point now) {
    vector<shared_ptr<Client>> late;
    for (const auto& [fd, c] : clients) {
        if (!c->authenticated && now - c->connected > AUTH_TIMEOUT) late.push_back(c);
    }
    for (const auto& c : late) schedule_close(c); // schedule_close modifica `clients`
}

// pompeaza inputul buffered ramas peste buget (declansat prin eventfd)
static void pump_work() {
    vector<shared_ptr<Client>> snapshot;
    snapshot.reserve(clients.size());
    for (const auto& [fd, c] : clients) snapshot.push_back(c);

    for (const auto& c : snapshot) {
        if (c->closed || c->in_buf.empty()) continue;
        if (serve(c)) queue_work(); // inca mai e lucru: re-signal
    }
}

int main() {
    // SEC-3: tot ce cream e doar al proprietarului (si 0600 explicit la fiecare open)
    umask(077);

    std::optional<Config> loaded = load_config();
    if (!loaded) return 1;
    cfg = std::move(*loaded);
    if (!load_tls()) return 1;
    net::init(cfg, g_tls);

    if (cfg.metrics) start_prometheus_exporter(9090, cfg.metrics_bind);
    signal(SIGPIPE, SIG_IGN);

    // loguri utile in fisiere si la moarte prin semnal
    setvbuf(stdout, nullptr, _IOLBF, 0);

    g_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_eventfd < 0) {
        cerr << "Nu pot crea eventfd: " << strerror(errno) << "\n";
        return 1;
    }

    g_server_fd = socket(AF_INET, SOCK_STREAM, 0);
    // listenerul RAMANE blocant: acceptul in ring e gestionat async de io-wq,
    // iar fd nonblocking ar produce re-inarmari in ciclu fara conexiuni
    int opt = 1;
    setsockopt(g_server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    inet_pton(AF_INET, cfg.bind.c_str(), &address.sin_addr); // validata de load_config
    address.sin_port = htons(static_cast<uint16_t>(cfg.port));

    if (bind(g_server_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
        cerr << "Nu pot face bind pe " << cfg.bind << ":" << cfg.port << ": " << strerror(errno) << "\n";
        close(g_server_fd);
        return 1;
    }

    struct io_uring ring;
    if (io_uring_queue_init(URING_ENTRIES, &ring, 0) < 0) {
        cerr << "Eroare la initializarea io_uring. Kernel prea vechi? :( \n";
        return 1;
    }

    // REF-1: datele se deschid abia dupa toate verificarile, deci o pornire gresita
    // (alt server pe port, configuratie invalida) nu atinge directorul de date.
    // Portul accepta conexiuni abia dupa replay-ul AOF, ca inainte.
    db.emplace(cfg);
    listen(g_server_fd, 4096);

    // mesajele Pub/Sub intra in cozile de output ale abonatilor
    db->set_message_sink(deliver_pubsub);

    arm_listener(&ring);
    arm_eventfd(&ring);

    cout << "Server pornit (io_uring" << (g_tls ? ", TLS" : "") << ") pe " << cfg.bind << ":" << cfg.port << "...\n";
    static Watchdog watchdog(*db);
    watchdog.start();

    // replicare CRDT catre SOMNIUM_PEERS (nimic fara peers)
    static Replicator replicator(db->replication_frontier(), cfg, g_tls);
    replicator.start();

    bool first_wait = true; // doar prima asteptare e sensibila la CQE-uri pierdute
    auto next_sweep = chrono::steady_clock::now();

    while (true) {
        submit_backlog(&ring);

        int ret;
        if (first_wait) {
            // prima asteptare: cu timeout - pe acest kernel am vazut CQE-ul
            // de listener pierdut la pornire (poll/accept armat corect,
            // conexiune in coada, si totusi nimic). La timeout re-submittm.
            __kernel_timespec wait_ts{3, 0};
            io_uring_cqe *first_cqe = nullptr;
            io_uring_submit(&ring);
            ret = io_uring_wait_cqe_timeout(&ring, &first_cqe, &wait_ts);
            if (ret == -ETIME) {
                if (cfg.trace_fd == -2) printf("[TRACE] prima asteptare timeout - resubmit armarilor\n");
                arm_listener(&ring);
                arm_eventfd(&ring);
                continue;
            }
            first_wait = false;
        } else {
            // cel mult o secunda de asteptare: bucla trebuie sa ajunga la sweep-ul de mai jos
            __kernel_timespec tick{1, 0};
            io_uring_cqe *ready = nullptr;
            ret = io_uring_submit_and_wait_timeout(&ring, &ready, 1, &tick, nullptr);
        }
        if (ret < 0 && ret != -EINTR && ret != -ETIME) {
            cerr << "io_uring_submit_and_wait esuat: " << strerror(-ret) << "\n";
            continue;
        }
        if (cfg.trace_fd == -2) printf("[TRACE] submit_and_wait ret=%d\n", ret);

        io_uring_cqe *cqe;
        unsigned head;
        unsigned count = 0;

        io_uring_for_each_cqe(&ring, head, cqe) {
            count++;
            auto* ticket = static_cast<PollTicket*>(io_uring_cqe_get_data(cqe));

            if (cfg.trace_fd == -2)
                printf("[TRACE] CQE res=%d ticket=%p\n", cqe->res, (void*)ticket);

            if (ticket == &listener_ticket) {
                const int res = cqe->res;
                if (res >= 0 && clients.size() >= cfg.maxclients) {
                    // SEC-6: fara Client, deci fara buffere; doar eroarea si inchiderea
                    static constexpr char kFull[] = "-ERR max number of clients reached\r\n";
                    ::send(res, kFull, sizeof(kFull) - 1, MSG_NOSIGNAL);
                    ::close(res);
                } else if (res >= 0) {
                    if (cfg.trace_fd == -2) printf("[TRACE] accept fd %d\n", res);
                    set_nonblocking(res); // SOCK_NONBLOCK deja; defensive
                    auto c = make_shared<Client>(res);
                    clients[res] = c;
                    arm_client(c, false);
                } else if (res != -EINTR) {
                    // accept esuat: logam si re-armam oricum, altfel serverul
                    // nu mai accepta conexiuni niciodata
                    printf("[TRACE] accept esuat: %d (%s)\n", res, strerror(-res));
                }
                arm_listener(&ring); // re-arm accept
                continue;
            }

            if (ticket == &eventfd_ticket) {
                arm_eventfd(&ring); // re-arm read, apoi pomparea
                pump_work();
                continue;
            }

            // ticket de client: shared_ptr garanteaza ca obiectul traieste pana aici
            const shared_ptr<Client> c = ticket->client;
            const bool for_write = ticket->for_write;
            delete ticket;

            if (c->closed) continue;

            if (for_write) {
                c->write_armed = false;
                const auto r = net::flush_output(*c);
                if (r == net::FlushResult::Pending) arm_client(c, true);
                else if (r == net::FlushResult::Dead) schedule_close(c);
            } else {
                c->read_armed = false;
                if (!net::drain_read(*c)) {
                    schedule_close(c);
                    continue;
                }
                if (serve(c)) {
                    queue_work(); // mai sunt comenzi buff-uite fara event nou
                } else if (c->in_buf.size() >= c->input_cap()) {
                    schedule_close(c); // o comanda incompleta a umplut bufferul
                }
            }
        }

        io_uring_cq_advance(&ring, count);

        if (const auto now = chrono::steady_clock::now(); now >= next_sweep) {
            close_unauthenticated(now);
            if (const unsigned failures = net::take_auth_failures()) { // SEC-20: cel mult o linie pe secunda
                printf("[AUTH] parole gresite: %u\n", failures);
            }
            next_sweep = now + chrono::seconds(1);
        }
    }
}
