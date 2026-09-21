#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <csignal>
#include <cerrno>
#include <sched.h>
#include <liburing.h>
#include <poll.h>
#include <sys/eventfd.h>

#include "metrics.h"
#include "src/core/database.h"
#include "watchdog.h"

using namespace std;

#define PORT 6379
#define URING_ENTRIES 4096
#define READ_CHUNK 16384
#define MAX_COMMANDS_PER_TURN 8192
#define MAX_INPUT_BUFFER (128ull * 1024 * 1024)
#define MAX_CLIENT_OUTPUT (32ull * 1024 * 1024)

static Database db;
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

    explicit Client(int f) : fd(f) {}
    size_t pending_out() const { return out_buf.size() - out_off; }
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
    if (c->pending_out() + data.size() > MAX_CLIENT_OUTPUT) {
        printf("[SlowClient] FD %d a depasit bufferul de output (%llu MB). Deconectat.\n",
               c->fd, static_cast<unsigned long long>(MAX_CLIENT_OUTPUT >> 20));
        schedule_close(c);
        return false;
    }
    c->out_buf += data;
    return true;
}

static void deliver_pubsub(const int fd, const string& msg) {
    const auto it = clients.find(fd);
    if (it == clients.end() || it->second->closed) return;
    if (!enqueue_output(it->second, msg)) return;

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

static vector<string> parse_resp(string& buffer);

// proceseaza comenzi buffered (pana la buget); true = mai exista input complet neprocesat
static bool process_buffered(const shared_ptr<Client>& c) {
    int processed = 0;
    bool budget_hit = false;

    while (true) {
        if (processed >= MAX_COMMANDS_PER_TURN) {
            budget_hit = !c->in_buf.empty();
            break;
        }
        vector<string> args = parse_resp(c->in_buf);
        if (args.empty()) break;

        const string response = db.execute(c->fd, args);
        ++processed;
        if (!response.empty()) {
            if (!enqueue_output(c, response)) return false; // deconectat (cap output)
        }
    }

    const auto r = flush_output(*c);
    if (r == FlushResult::Pending) arm_client(c, true);
    else if (r == FlushResult::Dead) {
        schedule_close(c);
        return false;
    }

    arm_client(c, false); // re-arm read
    return budget_hit;
}

// citeste tot ce e disponibil; false = conexiunea s-a terminat
static bool drain_read(const shared_ptr<Client>& c) {
    char temp[READ_CHUNK];
    while (c->in_buf.size() < MAX_INPUT_BUFFER) {
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

static vector<string> parse_resp(string& buffer) {
    vector<string> args;
    size_t pos = 0;

    if (buffer.empty()) return args;

    if (buffer[0] != '*') {
        buffer.clear();
        return args;
    }

    size_t crlf = buffer.find("\r\n", pos);
    if (crlf == string::npos) return args;

    try {
        const int num_args = stoi(buffer.substr(pos + 1, crlf - pos - 1));
        pos = crlf + 2;

        for (int i = 0; i < num_args; i++) {
            if (pos >= buffer.length()) return {};
            if (buffer[pos] != '$') {
                buffer.clear();
                return {};
            }

            crlf = buffer.find("\r\n", pos);
            if (crlf == string::npos) return {};

            const int len = stoi(buffer.substr(pos + 1, crlf - pos - 1));
            pos = crlf + 2;

            if (pos + len + 2 > buffer.length()) return {};

            args.push_back(buffer.substr(pos, len));
            pos += len + 2;
        }
        buffer.erase(0, pos);
        return args;
    } catch (...) {
        buffer.clear();
        return {};
    }
}

static void adjust_thread_priority(unsigned int current_load) {
    static bool is_high_prio = false;

    if (current_load > 500 && !is_high_prio) {
        sched_param param{};
        param.sched_priority = sched_get_priority_max(SCHED_FIFO);
        if (sched_setscheduler(0, SCHED_FIFO, &param) == 0) {
            is_high_prio = true;
            cout << "[PRIO CLIMB] Thread promovat la SCHED_FIFO (Real-Time) datorita stresului!\n";
        }
    }
    else if (current_load < 100 && is_high_prio) {
        sched_param param{};
        param.sched_priority = 0;
        if (sched_setscheduler(0, SCHED_OTHER, &param) == 0) {
            is_high_prio = false;
            cout << "[PRIO DROP] Trafic normalizat. Revenire la SCHED_OTHER.\n";
        }
    }
}

int main() {
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
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(g_server_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
        cerr << "Nu pot face bind pe portul " << port << ": " << strerror(errno) << "\n";
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

    cout << "Server pornit (io_uring) pe portul " << port << "...\n";
    watchdog.start();

    bool first_wait = true; // doar prima asteptare e sensibila la CQE-uri pierdute

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
            ret = io_uring_submit_and_wait(&ring, 1);
        }
        if (ret < 0 && ret != -EINTR) {
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
                if (res >= 0) {
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
                if (c->in_buf.size() > MAX_INPUT_BUFFER) {
                    schedule_close(c);
                    continue;
                }
                if (process_buffered(c)) {
                    queue_work(); // mai sunt comenzi buff-uite fara event nou
                }
            }
        }

        io_uring_cq_advance(&ring, count);

        adjust_thread_priority(count);
    }
}
