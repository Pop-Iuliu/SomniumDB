// Verificarea parserului RESP: header-only, ruleaza oriunde (fara io_uring).
#undef NDEBUG
#include "../src/core/resp.h"
#include <cassert>
#include <cstdio>

using resp::Status;

static Status parse_all(const std::string& buf, std::vector<std::string>* args = nullptr) {
    std::vector<std::string> scratch;
    size_t end = 0;
    return resp::parse(buf, 0, &end, args ? args : &scratch);
}

int main() {
    // cheie goala si o valoare care contine ea insasi CRLF
    const std::string cmd = "*3\r\n$3\r\nSET\r\n$0\r\n\r\n$5\r\nva\r\nl\r\n";
    std::vector<std::string> args;
    assert(parse_all(cmd, &args) == Status::Complete);
    assert((args == std::vector<std::string>{"SET", "", "va\r\nl"}));

    // orice prefix strict asteapta octeti: o comanda taiata oriunde intre
    // citiri nu e niciodata respinsa si nici executata de doua ori
    for (size_t cut = 0; cut < cmd.size(); ++cut) {
        assert(parse_all(cmd.substr(0, cut)) == Status::NeedMore);
    }

    // pipeline: comenzi consecutive citite de la offseturi
    const std::string pipeline = cmd + cmd + cmd;
    size_t pos = 0;
    size_t end = 0;
    int parsed = 0;
    while (resp::parse(pipeline, pos, &end, &args) == Status::Complete) {
        pos = end;
        ++parsed;
    }
    assert(parsed == 3 && pos == pipeline.size());

    assert(parse_all("*0\r\n", &args) == Status::Complete && args.empty());

    for (const char* bad : {
             "*1\r\n$-1\r\n",            // lungime negativa
             "*1\r\n$99999999999\r\n",   // peste kMaxBulk
             "*-1\r\n",                  // numar negativ de argumente
             "*2000000\r\n",             // peste kMaxArgs
             "*1\r\n$3\r\nabcXY",        // bulk fara CRLF final
             "*1\r\n:3\r\n",             // prefix gresit in array
             "+OK\r\n",                  // nu e un array
             "*+1\r\n", "*1 \r\n", "*\r\n",
             "*1111111111111111111111111111111111111", // header fara CRLF in fereastra
         }) {
        assert(parse_all(bad) == Status::Malformed);
    }

    // cererile inline: LF sau CRLF, spatii si taburi multiple, linia goala ignorata
    const std::string inline_cmds = "SET  a\tb\r\n\r\nGET a\n*1\r\n$4\r\nPING\r\n";
    std::vector<std::vector<std::string>> seen;
    pos = 0;
    while (resp::parse_request(inline_cmds, pos, &end, &args) == Status::Complete) {
        seen.push_back(args);
        pos = end;
    }
    assert(pos == inline_cmds.size());
    assert((seen == std::vector<std::vector<std::string>>{{"SET", "a", "b"}, {}, {"GET", "a"}, {"PING"}}));
    assert(resp::parse_request("GET a", 0, &end, &args) == Status::NeedMore);
    assert(resp::parse_request(std::string(resp::kMaxInline, 'x'), 0, &end, &args) == Status::Malformed);
    // AOF-ul ramane strict: aceeasi linie inline e respinsa de parse()
    assert(parse_all("GET a\r\n") == Status::Malformed);

    puts("resp_test: OK");
    return 0;
}
