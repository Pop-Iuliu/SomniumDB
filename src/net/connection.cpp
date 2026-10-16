#include "connection.h"
#include "auth.h"
#include "../core/database.h"
#include "../core/resp.h"
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/err.h>

namespace net {

namespace {
    constexpr size_t READ_CHUNK = 16384;
    constexpr int MAX_COMMANDS_PER_TURN = 8192;
    constexpr size_t MAX_INPUT_BUFFER = 128ull * 1024 * 1024;
    constexpr size_t MAX_UNAUTH_BUFFER = 16ull * 1024; // un strain nu are voie sa ne tina megaocteti (input sau output)

    Config g_cfg;                  // copia primita de init()
    SSL_CTX* g_tls = nullptr;      // SEC-16: SOMNIUM_TLS_CERT + SOMNIUM_TLS_KEY; nullptr = text clar
    uint64_t g_client_buffers = 0; // SEC-22: octetii din bufferele tuturor clientilor

    // SEC-16: TLS sta intre socket si buffere, pe BIO-uri de memorie. I/O-ul pe socket
    // ramane neschimbat; in_buf primeste text clar, out_buf textul cifrat gata de
    // trimis, deci capurile, bugetul si flush-ul lucreaza la fel ca fara TLS.
    SSL* tls_session() {
        SSL* ssl = SSL_new(g_tls);
        BIO* rbio = BIO_new(BIO_s_mem());
        BIO* wbio = BIO_new(BIO_s_mem());
        if (!ssl || !rbio || !wbio) abort(); // ca orice alocare esuata
        SSL_set_bio(ssl, rbio, wbio);
        SSL_set_accept_state(ssl);
        return ssl;
    }

    bool over_buffer_budget(const Client& c, const size_t extra) {
        if (g_client_buffers + extra <= g_cfg.max_client_buffers) return false;
        printf("[ClientBuffers] FD %d: bufferele clientilor depasesc SOMNIUM_MAX_CLIENT_BUFFERS. Deconectat.\n", c.fd);
        return true;
    }

    // ce a produs OpenSSL (handshake, inregistrari, alerte) trece in out_buf
    void take_ciphertext(Client& c) {
        BIO* wbio = SSL_get_wbio(c.ssl.get());
        char* data = nullptr;
        const long n = BIO_get_mem_data(wbio, &data);
        c.out_buf.append(data, static_cast<size_t>(n));
        (void)BIO_reset(wbio);
    }

    // octeti cifrati de pe socket -> text clar in in_buf; false = handshake esuat, alerta sau inchidere
    bool tls_in(Client& c, const char* raw, const size_t len) {
        BIO_write(SSL_get_rbio(c.ssl.get()), raw, static_cast<int>(len));
        char plain[READ_CHUNK];
        int n;
        while ((n = SSL_read(c.ssl.get(), plain, sizeof(plain))) > 0) c.in_buf.append(plain, static_cast<size_t>(n));
        take_ciphertext(c);
        if (SSL_get_error(c.ssl.get(), n) == SSL_ERROR_WANT_READ) return true;
        ERR_clear_error(); // coada de erori e a thread-ului: urmatorul client o vrea goala
        return false;
    }

    // text clar -> inregistrari TLS in out_buf
    bool tls_out(Client& c, const std::string& data) {
        const bool ok = SSL_write(c.ssl.get(), data.data(), static_cast<int>(data.size())) > 0;
        if (!ok) ERR_clear_error();
        take_ciphertext(c);
        return ok;
    }
} // namespace

void init(const Config& cfg, SSL_CTX* tls) {
    g_cfg = cfg;
    g_tls = tls;
}

Client::Client(const int f)
    : fd(f), authenticated(g_cfg.password.empty()), ssl(g_tls ? tls_session() : nullptr, SSL_free) {}

size_t Client::input_cap() const { return authenticated ? MAX_INPUT_BUFFER : MAX_UNAUTH_BUFFER; }
size_t Client::output_cap() const { return authenticated ? Database::MAX_REPLY : MAX_UNAUTH_BUFFER; }

// out_buf numara si partea deja trimisa: ramane alocata pana la golire
void track(Client& c) {
    const size_t now = c.closed ? 0 : c.in_buf.size() + c.out_buf.size();
    g_client_buffers = g_client_buffers - c.held + now;
    c.held = now;
}

FlushResult flush_output(Client& c) {
    while (c.out_off < c.out_buf.size()) {
        const ssize_t n = ::send(c.fd, c.out_buf.data() + c.out_off,
                                 c.out_buf.size() - c.out_off, MSG_NOSIGNAL);
        if (n > 0) {
            c.out_off += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (g_cfg.trace_fd == -2 || g_cfg.trace_fd == c.fd)
                printf("[TRACE] fd %d flush Pending la %zu/%zu\n", c.fd, c.out_off, c.out_buf.size());
            return FlushResult::Pending;
        }
        return FlushResult::Dead;
    }
    c.out_buf.clear();
    c.out_off = 0;
    track(c);
    return FlushResult::Complete;
}

// capul de output: clientii prea lenti sunt deconectati ca sa nu moara serverul
bool enqueue_output(Client& c, const std::string& data) {
    if (c.closed) return false;
    if (c.pending_out() + data.size() > c.output_cap()) {
        printf("[SlowClient] FD %d a depasit bufferul de output (%llu KB). Deconectat.\n",
               c.fd, static_cast<unsigned long long>(c.output_cap() >> 10));
        return false;
    }
    if (over_buffer_budget(c, data.size())) return false;
    if (!c.ssl) c.out_buf += data;
    else if (!tls_out(c, data)) return false;
    track(c);
    return true;
}

Step process_buffered(Client& c, Database& db) {
    // parsam de la un offset si stergem inputul consumat o singura data pe
    // batch: un pipeline adanc costa liniar, nu cate o copiere per comanda
    int processed = 0;
    size_t pos = 0;
    std::vector<std::string> args;
    resp::Status st = resp::Status::NeedMore;
    bool quit = false;

    while (processed < MAX_COMMANDS_PER_TURN) {
        size_t end = 0;
        st = resp::parse_request(c.in_buf, pos, &end, &args);
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
        const std::string response = strcasecmp(name, "AUTH") == 0    ? auth(c, args, g_cfg.password)
                                   : strcasecmp(name, "HELLO") == 0   ? hello(c, args, g_cfg.password)
                                   : !c.authenticated                 ? "-NOAUTH Authentication required.\r\n"
                                                                      : db.execute(c.fd, args, c.resp3);
        if (!response.empty() && !enqueue_output(c, response)) return Step::Drop; // un cap depasit
    }
    c.in_buf.erase(0, pos);
    track(c);

    // ca Redis: dupa QUIT sau o eroare de protocol (inputul nu mai poate fi
    // resincronizat) trimitem ultimul raspuns si inchidem
    if (quit || st == resp::Status::Malformed) {
        if (enqueue_output(c, quit ? "+OK\r\n" : "-ERR Protocol error\r\n")) flush_output(c);
        return Step::Drop;
    }

    // ce nu pleaca acum ramane in out_buf, iar bucla armeaza POLLOUT pentru el
    if (flush_output(c) == FlushResult::Dead) return Step::Drop;
    return processed == MAX_COMMANDS_PER_TURN && !c.in_buf.empty() ? Step::More : Step::Done;
}

bool drain_read(Client& c) {
    char temp[READ_CHUNK];
    while (c.in_buf.size() < c.input_cap()) {
        const ssize_t n = read(c.fd, temp, sizeof(temp));
        if (n > 0) {
            if (!c.ssl) c.in_buf.append(temp, static_cast<size_t>(n));
            else if (!tls_in(c, temp, static_cast<size_t>(n))) return false;
            track(c);
            if (over_buffer_budget(c, 0)) return false;
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

} // namespace net
