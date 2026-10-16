#include "auth.h"
#include "connection.h"
#include <charconv>
#include <strings.h>
#include <system_error>

namespace net {

namespace {
    unsigned g_auth_failures = 0; // SEC-20: parole gresite inca nelogate

    // Parola se compara in timp constant: bucla merge mereu pe toata lungimea
    // asteptata, fara iesire timpurie.
    bool credentials_ok(const std::string& user, const std::string& pass, const std::string& password) {
        unsigned char diff = pass.size() != password.size();
        for (size_t i = 0; i < password.size(); ++i) {
            diff |= static_cast<unsigned char>(password[i] ^ (i < pass.size() ? pass[i] : 0));
        }
        return diff == 0 && user == "default";
    }

    std::string authenticate(Client& c, const std::string& user, const std::string& pass, const std::string& password) {
        if (!credentials_ok(user, pass, password)) {
            ++g_auth_failures; // logate agregat de sweep-ul din bucla
            return "-WRONGPASS invalid username-password pair\r\n";
        }
        c.authenticated = true;
        return "+OK\r\n";
    }
} // namespace

std::string auth(Client& c, const std::vector<std::string>& args, const std::string& password) {
    if (args.size() != 2 && args.size() != 3) return "-ERR wrong number of arguments for 'auth' command\r\n";
    if (password.empty()) {
        return "-ERR AUTH <password> called without any password configured for the default user. "
               "Are you sure your configuration is correct?\r\n";
    }
    return authenticate(c, args.size() == 3 ? args[1] : "default", args.back(), password);
}

// Fara parola configurata AUTH e acceptat ca pentru utilizatorul default fara
// parola; SETNAME e ignorat. Raspunsul: harta pe RESP3, lista plata pe RESP2.
// "version" e nivelul de protocol Redis pe care il imitam.
std::string hello(Client& c, const std::vector<std::string>& args, const std::string& password) {
    int proto = c.resp3 ? 3 : 2;
    if (args.size() >= 2) {
        const std::string& v = args[1];
        if (const auto [p, ec] = std::from_chars(v.data(), v.data() + v.size(), proto); ec != std::errc() || p != v.data() + v.size()) {
            return "-ERR Protocol version is not an integer or out of range\r\n";
        }
        if (proto != 2 && proto != 3) return "-NOPROTO unsupported protocol version\r\n";
        for (size_t i = 2; i < args.size();) {
            if (strcasecmp(args[i].c_str(), "AUTH") == 0 && i + 2 < args.size()) {
                if (!password.empty()) {
                    if (std::string r = authenticate(c, args[i + 1], args[i + 2], password); r[0] == '-') return r;
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

    const auto bulk = [](const std::string& s) { return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n"; };
    return (c.resp3 ? "%7\r\n" : "*14\r\n") + bulk("server") + bulk("somnium") + bulk("version") + bulk("7.0.0") +
           bulk("proto") + ":" + (c.resp3 ? "3" : "2") + "\r\n" + bulk("id") + ":" + std::to_string(c.fd) + "\r\n" +
           bulk("mode") + bulk("standalone") + bulk("role") + bulk("master") + bulk("modules") + "*0\r\n";
}

unsigned take_auth_failures() {
    const unsigned n = g_auth_failures;
    g_auth_failures = 0;
    return n;
}

} // namespace net
