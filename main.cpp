#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <cstring>
#include <strings.h>
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

#include "metrics.h"
#include "src/core/database.h"
#include "src/core/resp.h"
#include "src/storage/fs_util.h"
#include "src/storage/replicator.h"
#include "watchdog.h"

using namespace std;

#define PORT 6379
#define URING_ENTRIES 4096
#define READ_CHUNK 16384
#define MAX_COMMANDS_PER_TURN 8192
#define MAX_INPUT_BUFFER (128ull * 1024 * 1024)
#define MAX_UNAUTH_BUFFER (16ull * 1024) // un strain nu are voie sa ne tina megaocteti (input sau output)
#define MAX_CLIENT_OUTPUT (32ull * 1024 * 1024)
static constexpr auto AUTH_TIMEOUT = chrono::seconds(10); // SEC-18: cat poate tine un strain un loc

static Database db;
static string g_password; // SEC-2: SOMNIUM_PASSWORD; gol = fara autentificare
static uint64_t g_maxclients = 10000; // SEC-6: SOMNIUM_MAXCLIENTS, ca Redis
static Watchdog watchdog(db);

// S3 "naigie": calutul de munca care nu se opreste pentru niciun client.
// Fiecare client are o coada de output ordonata; scrierile partiale se reiau
// pe evenimente io_uring POLLOUT, niciodata prin asteptare blocanta in timpul
// unei comenzi. Clientii care nu urmeaza citirea sunt deconectati la un cap.

struct Client {
    int fd;
    string in_buf;
    string out_buf;
    size_t out_off = 0; // bytes deja plecati din out_buf
    bool closed = false;
    bool read_armed = false;
    bool write_armed = false;
    bool resp3 = false; // protocolul negociat prin HELLO
    bool authenticated = g_password.empty();
    const chrono::steady_clock::time_point connected = chrono::steady_clock::now();

    explicit Client(int f) : fd(f) {}
    size_t pending_out() const { return out_buf.size() - out_off; }
    size_t input_cap() const { return authenticated ? MAX_INPUT_BUFFER : MAX_UNAUTH_BUFFER; }
    size_t output_cap() const { return authenticated ? MAX_CLIENT_OUTPUT : MAX_UNAUTH_BUFFER; }
};

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
static int g_trace = -1; // fd cu trace activ (debug), -1 = off

static void set_nonblocking(const int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void schedule_close(const shared_ptr<Client>& c) {
    if (c->closed) return;
    c->closed = true;
    if (g_trace == -2 || g_trace == c->fd)
        printf("[TRACE] fd %d CLOSE\n", c->fd);
    db.cleanup_client(c->fd);
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
    if (g_trace == -2 || g_trace == c->fd)
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
        if (g_trace == -2) printf("[TRACE] arm_listener ESUAT (sqe null)\n");
        return;
    }
    io_uring_prep_accept(sqe, g_server_fd, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
    io_uring_sqe_set_data(sqe, &listener_ticket);
    const int submitted = io_uring_submit(ring);
    if (g_trace == -2) printf("[TRACE] arm_listener submit=%d\n", submitted);
}

static void arm_eventfd(io_uring* ring) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) return;
    // read blocant in ring: CQE doar cand contorul eventfd e nenul
    io_uring_prep_read(sqe, g_eventfd, &eventfd_drain_buffer, sizeof(eventfd_drain_buffer), 0);
    io_uring_sqe_set_data(sqe, &eventfd_ticket);
    const int submitted = io_uring_submit(ring);
    if (g_trace == -2) printf("[TRACE] arm_eventfd submit=%d\n", submitted);
}

// tri-state: Complete = tot a plecat; Pending = fd plin, reluam pe POLLOUT; Dead = eroare
enum class FlushResult { Complete, Pending, Dead };

static FlushResult flush_output(Client& c) {
    while (c.out_off < c.out_buf.size()) {
        const ssize_t n = ::send(c.fd, c.out_buf.data() + c.out_off,
                                 c.out_buf.size() - c.out_off, MSG_NOSIGNAL);
        if (n > 0) {
            c.out_off += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (g_trace == -2 || g_trace == c.fd)
                printf("[TRACE] fd %d flush Pending la %zu/%zu\n", c.fd, c.out_off, c.out_buf.size());
            return FlushResult::Pending;
        }
        return FlushResult::Dead;
    }
    c.out_buf.clear();
    c.out_off = 0;
    return FlushResult::Complete;
}

// capul de output: deconecteaza clientii prea lenti ca sa nu moara serverul
static bool enqueue_output(const shared_ptr<Client>& c, const string& data) {
    if (c->closed) return false;
    if (c->pending_out() + data.size() > c->output_cap()) {
        printf("[SlowClient] FD %d a depasit bufferul de output (%llu KB). Deconectat.\n",
               c->fd, static_cast<unsigned long long>(c->output_cap() >> 10));
        schedule_close(c);
        return false;
    }
    c->out_buf += data;
    return true;
}

static void deliver_pubsub(const int fd, const string& msg) {
    const auto it = clients.find(fd);
    if (it == clients.end() || it->second->closed) return;
    if (it->second->resp3) {
        // RESP3: acelasi mesaj, trimis ca push ('>' in loc de '*'), ca sa nu se
        // confunde cu raspunsul unei comenzi trimise de abonat intre timp
        string push = msg;
        push[0] = '>';
        if (!enqueue_output(it->second, push)) return;
    } else if (!enqueue_output(it->second, msg)) {
        return;
    }

    // incercam trimisul direct; daca fd-ul e plin, POLLOUT il ia mai tarziu
    const auto r = flush_output(*it->second);
    if (r == FlushResult::Pending) arm_client(it->second, true);
    else if (r == FlushResult::Dead) schedule_close(it->second);
}

static void queue_work() {
    uint64_t one = 1;
    if (write(g_eventfd, &one, sizeof(one)) < 0 && errno != EAGAIN) {
        cerr << "eventfd write esuat: " << strerror(errno) << "\n";
    }
}

// SEC-2: un singur utilizator, "default". Parola se compara in timp constant:
// bucla merge mereu pe toata lungimea asteptata, fara iesire timpurie.
static bool credentials_ok(const string& user, const string& pass) {
    unsigned char diff = pass.size() != g_password.size();
    for (size_t i = 0; i < g_password.size(); ++i) {
        diff |= static_cast<unsigned char>(g_password[i] ^ (i < pass.size() ? pass[i] : 0));
    }
    return diff == 0 && user == "default";
}

static string authenticate(Client& c, const string& user, const string& pass) {
    if (!credentials_ok(user, pass)) {
        printf("[AUTH] fd %d: parola gresita\n", c.fd);
        return "-WRONGPASS invalid username-password pair\r\n";
    }
    c.authenticated = true;
    return "+OK\r\n";
}

// AUTH [user] pass
static string auth(Client& c, const vector<string>& args) {
    if (args.size() != 2 && args.size() != 3) return "-ERR wrong number of arguments for 'auth' command\r\n";
    if (g_password.empty()) {
        return "-ERR AUTH <password> called without any password configured for the default user. "
               "Are you sure your configuration is correct?\r\n";
    }
    return authenticate(c, args.size() == 3 ? args[1] : "default", args.back());
}

// HELLO [protover [AUTH user pass] [SETNAME name]]: protocolul tine de conexiune.
// Fara parola configurata AUTH e acceptat ca pentru utilizatorul default fara
// parola; SETNAME e ignorat. Raspunsul: harta pe RESP3, lista plata pe RESP2.
// "version" e nivelul de protocol Redis pe care il imitam.
static string hello(Client& c, const vector<string>& args) {
    int proto = c.resp3 ? 3 : 2;
    if (args.size() >= 2) {
        const string& v = args[1];
        if (const auto [p, ec] = from_chars(v.data(), v.data() + v.size(), proto); ec != errc() || p != v.data() + v.size()) {
            return "-ERR Protocol version is not an integer or out of range\r\n";
        }
        if (proto != 2 && proto != 3) return "-NOPROTO unsupported protocol version\r\n";
        for (size_t i = 2; i < args.size();) {
            if (strcasecmp(args[i].c_str(), "AUTH") == 0 && i + 2 < args.size()) {
                if (!g_password.empty()) {
                    if (string r = authenticate(c, args[i + 1], args[i + 2]); r[0] == '-') return r;
                }
                i += 3;
            } else if (strcasecmp(args[i].c_str(), "SETNAME") == 0 && i + 1 < args.size()) {
                i += 2;
            } else {
                return "-ERR Syntax error in HELLO option '" + args[i] + "'\r\n";
            }
        }
    }
    if (!c.authenticated) {
        return "-NOAUTH HELLO must be called with the client already authenticated, otherwise the "
               "HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and "
               "select the RESP protocol version at the same time\r\n";
    }
    c.resp3 = proto == 3;

    const auto bulk = [](const string& s) { return "$" + to_string(s.size()) + "\r\n" + s + "\r\n"; };
    return (c.resp3 ? "%7\r\n" : "*14\r\n") + bulk("server") + bulk("somnium") + bulk("version") + bulk("7.0.0") +
           bulk("proto") + ":" + (c.resp3 ? "3" : "2") + "\r\n" + bulk("id") + ":" + to_string(c.fd) + "\r\n" +
           bulk("mode") + bulk("standalone") + bulk("role") + bulk("master") + bulk("modules") + "*0\r\n";
}

// proceseaza comenzi buffered (pana la buget); true = mai exista input complet neprocesat
static bool process_buffered(const shared_ptr<Client>& c) {
    // parsam de la un offset si stergem inputul consumat o singura data pe
    // batch: un pipeline adanc costa liniar, nu cate o copiere per comanda
    int processed = 0;
    size_t pos = 0;
    vector<string> args;
    resp::Status st = resp::Status::NeedMore;
    bool quit = false;

    while (processed < MAX_COMMANDS_PER_TURN) {
        size_t end = 0;
        st = resp::parse_request(c->in_buf, pos, &end, &args);
        if (st != resp::Status::Complete) break;
        pos = end;
        ++processed;

        // QUIT tine de conexiune, nu de baza de date: raspundem si inchidem
        if (args.size() == 1 && strcasecmp(args[0].c_str(), "QUIT") == 0) {
            quit = true;
            break;
        }

        // AUTH si HELLO tin de conexiune; HELLO alege protocolul in care raspund
        // celelalte comenzi. Pana la autentificare, doar ele (si QUIT) trec.
        const char* name = args.empty() ? "" : args[0].c_str();
        const string response = strcasecmp(name, "AUTH") == 0    ? auth(*c, args)
                              : strcasecmp(name, "HELLO") == 0   ? hello(*c, args)
                              : !c->authenticated                ? "-NOAUTH Authentication required.\r\n"
                                                                 : db.execute(c->fd, args, c->resp3);
        if (!response.empty() && !enqueue_output(c, response)) return false; // deconectat (cap output)
    }
    c->in_buf.erase(0, pos);

    // ca Redis: dupa QUIT sau o eroare de protocol (inputul nu mai poate fi
    // resincronizat) trimitem ultimul raspuns si inchidem
    if (quit || st == resp::Status::Malformed) {
        if (enqueue_output(c, quit ? "+OK\r\n" : "-ERR Protocol error\r\n")) flush_output(*c);
        schedule_close(c);
        return false;
    }

    const auto r = flush_output(*c);
    if (r == FlushResult::Pending) arm_client(c, true);
    else if (r == FlushResult::Dead) {
        schedule_close(c);
        return false;
    }

    arm_client(c, false); // re-arm read
    return processed == MAX_COMMANDS_PER_TURN && !c->in_buf.empty();
}

// citeste tot ce e disponibil; false = conexiunea s-a terminat
static bool drain_read(const shared_ptr<Client>& c) {
    char temp[READ_CHUNK];
    while (c->in_buf.size() < c->input_cap()) {
        const ssize_t n = read(c->fd, temp, sizeof(temp));
        if (n > 0) {
            c->in_buf.append(temp, static_cast<size_t>(n));
            if (static_cast<size_t>(n) < sizeof(temp)) return true; // s-a golit kernel bufferul
            continue;
        }
        if (n == 0) return false; // EOF
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
        return false;
    }
    return true; // cap de input: restul asteapta urmatorul ciclu
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
        if (process_buffered(c)) queue_work(); // inca mai e lucru: re-signal
    }
}

int main() {
    // SEC-3: tot ce cream de aici incolo e doar al proprietarului. Fisierele
    // deschise la initializarea statica (AOF, cold.bin) au deja 0600 explicit.
    umask(077);

    // portul poate fi suprascris (ex: masina de dev are deja un redis pe 6379)
    int port = PORT;
    if (const char* env_port = getenv("REDIS_PORT"); env_port != nullptr) {
        try {
            port = stoi(env_port);
        } catch (...) {
            cerr << "REDIS_PORT invalid: " << env_port << "\n";
            return 1;
        }
    }

    if (const char* env = getenv("SOMNIUM_MAXCLIENTS"); env && (!fsutil::parse_u64(env, &g_maxclients) || g_maxclients == 0)) {
        cerr << "SOMNIUM_MAXCLIENTS invalid: " << env << "\n";
        return 1;
    }

    // SEC-1: implicit doar localhost; expunerea in retea e o decizie explicita
    const char* bind_env = getenv("SOMNIUM_BIND");
    const string bind_ip = bind_env ? bind_env : "127.0.0.1";
    in_addr bind_addr{};
    if (inet_pton(AF_INET, bind_ip.c_str(), &bind_addr) != 1) {
        cerr << "SOMNIUM_BIND invalid: " << bind_ip << " (astept o adresa IPv4)\n";
        return 1;
    }
    if (const char* pw = getenv("SOMNIUM_PASSWORD")) g_password = pw;
    if (ntohl(bind_addr.s_addr) >> 24 != 127 && g_password.empty()) {
        cerr << "Refuz sa pornesc: SOMNIUM_BIND=" << bind_ip
             << " expune serverul in retea, dar SOMNIUM_PASSWORD nu e setat\n";
        return 1;
    }

    if (getenv("SOMNIUM_NO_METRICS") == nullptr) {
        start_prometheus_exporter(9090);
    }
    signal(SIGPIPE, SIG_IGN);

    if (const char* t = getenv("SOMNIUM_TRACE_FD"); t != nullptr) g_trace = atoi(t);
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
    address.sin_addr = bind_addr;
    address.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(g_server_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
        cerr << "Nu pot face bind pe " << bind_ip << ":" << port << ": " << strerror(errno) << "\n";
        close(g_server_fd);
        return 1;
    }
    listen(g_server_fd, 4096);

    struct io_uring ring;
    if (io_uring_queue_init(URING_ENTRIES, &ring, 0) < 0) {
        cerr << "Eroare la initializarea io_uring. Kernel prea vechi? :( \n";
        return 1;
    }

    // mesajele Pub/Sub intra in cozile de output ale abonatilor
    db.set_message_sink(deliver_pubsub);

    arm_listener(&ring);
    arm_eventfd(&ring);

    cout << "Server pornit (io_uring) pe " << bind_ip << ":" << port << "...\n";
    watchdog.start();

    // replicare CRDT catre SOMNIUM_PEERS (nimic fara peers)
    static Replicator replicator(db.replication_frontier());
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
                if (g_trace == -2) printf("[TRACE] prima asteptare timeout - resubmit armarilor\n");
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
        if (g_trace == -2) printf("[TRACE] submit_and_wait ret=%d\n", ret);

        io_uring_cqe *cqe;
        unsigned head;
        unsigned count = 0;

        io_uring_for_each_cqe(&ring, head, cqe) {
            count++;
            auto* ticket = static_cast<PollTicket*>(io_uring_cqe_get_data(cqe));

            if (g_trace == -2)
                printf("[TRACE] CQE res=%d ticket=%p\n", cqe->res, (void*)ticket);

            if (ticket == &listener_ticket) {
                const int res = cqe->res;
                if (res >= 0 && clients.size() >= g_maxclients) {
                    // SEC-6: fara Client, deci fara buffere; doar eroarea si inchiderea
                    static constexpr char kFull[] = "-ERR max number of clients reached\r\n";
                    ::send(res, kFull, sizeof(kFull) - 1, MSG_NOSIGNAL);
                    ::close(res);
                } else if (res >= 0) {
                    if (g_trace == -2) printf("[TRACE] accept fd %d\n", res);
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
                const auto r = flush_output(*c);
                if (r == FlushResult::Pending) arm_client(c, true);
                else if (r == FlushResult::Dead) schedule_close(c);
            } else {
                c->read_armed = false;
                if (!drain_read(c)) {
                    schedule_close(c);
                    continue;
                }
                if (process_buffered(c)) {
                    queue_work(); // mai sunt comenzi buff-uite fara event nou
                } else if (c->in_buf.size() >= c->input_cap()) {
                    schedule_close(c); // o comanda incompleta a umplut bufferul
                }
            }
        }

        io_uring_cq_advance(&ring, count);

        if (const auto now = chrono::steady_clock::now(); now >= next_sweep) {
            close_unauthenticated(now);
            next_sweep = now + chrono::seconds(1);
        }
    }
}
