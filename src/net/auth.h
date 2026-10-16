#pragma once

// SEC-2: autentificarea conexiunii. Un singur utilizator, "default"; parola e
// SOMNIUM_PASSWORD (goala = fara autentificare).

#include <string>
#include <vector>

namespace net {

struct Client;

// AUTH [user] pass
std::string auth(Client& c, const std::vector<std::string>& args, const std::string& password);

// HELLO [protover [AUTH user pass] [SETNAME name]]: protocolul tine de conexiune
std::string hello(Client& c, const std::vector<std::string>& args, const std::string& password);

// SEC-20: parolele gresite de la ultimul apel; sweep-ul din bucla le logheaza agregat
unsigned take_auth_failures();

} // namespace net
