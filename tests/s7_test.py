"""S7 "catholicized": orice client Redis e binevenit.

Verifica: comenzi inline (cu ghilimele), PING/ECHO/QUIT, EXISTS/MGET/MSET
(cold storage inclus, expiratele absente), EXPIRE/PEXPIRE/PERSIST cu termen
absolut peste restart si optiunile NX/XX/GT/LT, SUBSCRIBE multiplu +
UNSUBSCRIBE + PING in modul subscribe, RESP3 prin HELLO, si redis-cli /
redis-benchmark reale cand sunt instalate (in CI sunt).
"""

import os
import shutil
import subprocess
import time

from common import Client, Server, report, resp_cmd, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}


def read_to_eof(s, timeout=10):
    s.settimeout(timeout)
    data = b""
    while True:
        chunk = s.recv(1 << 16)
        if not chunk:
            return data
        data += chunk


def sec_basics(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.s.sendall(b"PING\r\nSET  greeting\thello\r\n\r\nGET greeting\n")
        check("PING inline", c.read_reply() == ("ok", "PONG"))
        check("SET inline cu spatii si taburi", c.read_reply() == ("ok", "OK"))
        check("GET inline terminat cu LF", c.read_reply() == ("bulk", b"hello"))
        check("PING cu mesaj", c.cmd("PING", "hi") == ("bulk", b"hi"))
        check("ECHO", c.cmd("ECHO", "x y") == ("bulk", b"x y"))

        check("MSET", c.cmd("MSET", "k1", "v1", "k2", "v2", "k1", "v3") == ("ok", "OK"))
        check("MSET cu argumente impare", c.cmd("MSET", "k1", "v1", "k2")[0] == "err")
        check("MGET", c.cmd("MGET", "k1", "missing", "k2") ==
              ("array", [("bulk", b"v3"), ("nil", None), ("bulk", b"v2")]))
        check("EXISTS numara si repetitiile", c.cmd("EXISTS", "k1", "k1", "missing", "k2") == ("int", 3))

        check("EXPIRE pe cheie absenta", c.cmd("EXPIRE", "missing", "10") == ("int", 0))
        check("EXPIRE", c.cmd("EXPIRE", "k1", "100") == ("int", 1))
        check("TTL dupa EXPIRE", c.cmd("TTL", "k1") == ("int", 100))
        check("PERSIST", c.cmd("PERSIST", "k1") == ("int", 1))
        check("TTL dupa PERSIST", c.cmd("TTL", "k1") == ("int", -1))
        check("PERSIST pe cheie fara termen", c.cmd("PERSIST", "k1") == ("int", 0))
        check("EXPIRE invalid", c.cmd("EXPIRE", "k1", "abc")[0] == "err")
        check("EXPIRE 0 sterge cheia", c.cmd("EXPIRE", "k2", "0") == ("int", 1))
        check("cheia stearsa de EXPIRE 0", c.cmd("EXISTS", "k2") == ("int", 0))
        c.cmd("SET", "e", "v", "PX", "100")
        time.sleep(0.3)
        check("MGET: cheie expirata absenta", c.cmd("MGET", "e") == ("array", [("nil", None)]))
        check("EXISTS: cheie expirata absenta", c.cmd("EXISTS", "e") == ("int", 0))

        q = srv.connect()
        q.sendall(resp_cmd("QUIT") + resp_cmd("SET", "after-quit", "1"))
        check("QUIT raspunde +OK si inchide", read_to_eof(q) == b"+OK\r\n")
        q.close()
        check("nimic dupa QUIT nu se executa", c.cmd("EXISTS", "after-quit") == ("int", 0))
        c.close()
    finally:
        srv.cleanup()


def sec_cold_and_restart(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_MAX_KEYS": "0"})  # totul ajunge in cold storage
    try:
        srv.start()
        c = Client(srv)
        c.cmd("MSET", "a", "1", "b", "2")
        check("MGET vede cheile din cold", c.cmd("MGET", "a", "b") == ("array", [("bulk", b"1"), ("bulk", b"2")]))
        c.cmd("SET", "other", "x")  # a si b inapoi in cold
        check("EXISTS vede cheile din cold", c.cmd("EXISTS", "a", "b") == ("int", 2))
        c.cmd("SET", "other", "y")
        check("PEXPIRE pe cheie din cold", c.cmd("PEXPIRE", "a", "30000") == ("int", 1))
        p = c.cmd("PTTL", "a")[1]
        c.close()

        srv.env_extra["SOMNIUM_CLOCK_OFFSET_MS"] = "20000"  # 20s trec cat serverul e oprit
        srv.restart()
        c = Client(srv)
        p2 = c.cmd("PTTL", "a")[1]
        check("PEXPIRE: termen absolut peste restart", 0 < p2 <= p - 20000, (p, p2))
        check("MSET redat dupa restart", c.cmd("GET", "b") == ("bulk", b"2"))
        c.close()
    finally:
        srv.cleanup()


def sec_pubsub(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        sub = Client(srv)
        sub.s.sendall(resp_cmd("SUBSCRIBE", "a", "b"))
        first, second = sub.read_reply(), sub.read_reply()
        check("SUBSCRIBE la doua canale", first[1][2] == ("int", 1) and second[1][2] == ("int", 2), (first, second))
        check("PING in modul subscribe", sub.cmd("PING") == ("array", [("bulk", b"pong"), ("bulk", b"")]))
        check("comenzile normale refuzate in modul subscribe", sub.cmd("GET", "x")[0] == "err")

        sub.s.sendall(resp_cmd("UNSUBSCRIBE"))
        replies = [sub.read_reply(), sub.read_reply()]
        check("UNSUBSCRIBE de la toate canalele", sorted(r[1][2][1] for r in replies) == [0, 1], replies)
        check("dupa UNSUBSCRIBE: comenzi normale", sub.cmd("SET", "x", "1") == ("ok", "OK"))
        check("UNSUBSCRIBE fara abonamente", sub.cmd("UNSUBSCRIBE") ==
              ("array", [("bulk", b"unsubscribe"), ("nil", None), ("int", 0)]))

        pub = Client(srv)
        check("PUBLISH dupa dezabonare: niciun receptor", pub.cmd("PUBLISH", "a", "m") == ("int", 0))
        pub.close()
        sub.close()
    finally:
        srv.cleanup()


def sec_inline_quotes(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.s.sendall(b'SET q1 "hello world"\r\n'
                    + rb'SET q2 "a\x41\n\"b"' + b"\r\n"
                    + rb"SET q3 'it\'s'" + b"\r\n"
                    + b'SET q4 x"y z"\r\n')
        check("SET-uri inline cu ghilimele", [c.read_reply() for _ in range(4)] == [("ok", "OK")] * 4)
        check("ghilimele duble pastreaza spatiile", c.cmd("GET", "q1") == ("bulk", b"hello world"))
        check("escape-uri in ghilimele duble", c.cmd("GET", "q2") == ("bulk", b'aA\n"b'))
        check("ghilimea escapata in ghilimele simple", c.cmd("GET", "q3") == ("bulk", b"it's"))
        check("ghilimele in mijlocul argumentului", c.cmd("GET", "q4") == ("bulk", b"xy z"))
        c.close()

        for name, line in (("ghilimele neinchise", b'SET q "abc\r\n'), ("ghilimea lipita de text", b'SET q "ab"c\r\n')):
            bad = srv.connect()
            bad.sendall(line)
            check(f"{name}: eroare de protocol", read_to_eof(bad) == b"-ERR Protocol error\r\n")
            bad.close()
    finally:
        srv.cleanup()


def sec_expire_options(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "p", "v")
        check("XX pe cheie fara termen", c.cmd("EXPIRE", "p", "100", "XX") == ("int", 0))
        check("GT pe cheie fara termen (TTL infinit)", c.cmd("EXPIRE", "p", "100", "GT") == ("int", 0))
        check("LT pe cheie fara termen (TTL infinit)", c.cmd("EXPIRE", "p", "100", "LT") == ("int", 1))
        check("NX pe cheie cu termen", c.cmd("EXPIRE", "p", "50", "NX") == ("int", 0))
        check("GT cu termen mai lung", c.cmd("EXPIRE", "p", "200", "GT") == ("int", 1))
        check("GT cu termen mai scurt", c.cmd("EXPIRE", "p", "150", "gt") == ("int", 0))
        check("TTL dupa GT", c.cmd("TTL", "p") == ("int", 200))
        check("LT cu termen mai scurt", c.cmd("EXPIRE", "p", "150", "LT") == ("int", 1))
        check("PEXPIRE XX GT", c.cmd("PEXPIRE", "p", "300000", "XX", "GT") == ("int", 1))
        check("TTL dupa PEXPIRE XX GT", c.cmd("TTL", "p") == ("int", 300))
        for opts in (("NX", "XX"), ("NX", "GT"), ("GT", "LT"), ("FOO",)):
            check(f"EXPIRE {' '.join(opts)} -> -ERR", c.cmd("EXPIRE", "p", "10", *opts)[0] == "err")
        check("optiunile se verifica inainte de stergere (GT)", c.cmd("EXPIRE", "p", "0", "GT") == ("int", 0))
        check("cheia ramane dupa GT refuzat", c.cmd("EXISTS", "p") == ("int", 1))
        check("LT cu termen trecut sterge cheia", c.cmd("EXPIRE", "p", "-1", "LT") == ("int", 1))
        check("cheia stearsa de LT", c.cmd("EXISTS", "p") == ("int", 0))
        c.cmd("SET", "n", "v")
        check("NX pe cheie fara termen", c.cmd("EXPIRE", "n", "10", "NX") == ("int", 1))
        c.close()
    finally:
        srv.cleanup()


def sec_resp3(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        r = c.cmd("HELLO")
        check("HELLO fara argumente: RESP2, lista plata", r[0] == "array" and len(r[1]) == 14 and r[1][5] == ("int", 2), r)
        r = c.cmd("HELLO", "3")
        fields = {k[1]: v for k, v in r[1]} if r[0] == "map" else {}
        check("HELLO 3: harta cu proto 3", fields.get(b"proto") == ("int", 3) and fields.get(b"server") == ("bulk", b"somnium"), r)
        check("RESP3: GET absent -> null", c.cmd("GET", "nope") == ("null", None))
        c.cmd("SET", "a", "1")
        check("RESP3: MGET cu null", c.cmd("MGET", "a", "nope") == ("array", [("bulk", b"1"), ("null", None)]))
        check("RESP3: SET NX esuat -> null", c.cmd("SET", "a", "2", "NX") == ("null", None))
        check("RESP3: ROOM.INFO e harta", c.cmd("ROOM.INFO", "default")[0] == "map")
        check("HELLO cu protocol nesuportat", c.cmd("HELLO", "4") == ("err", "NOPROTO unsupported protocol version"))
        check("HELLO cu versiune invalida", c.cmd("HELLO", "x")[0] == "err")
        check("HELLO cu optiune necunoscuta", c.cmd("HELLO", "3", "FOO")[0] == "err")
        check("HELLO 3 AUTH ... SETNAME ...", c.cmd("HELLO", "3", "AUTH", "default", "pw", "SETNAME", "me")[0] == "map")
        c.cmd("HELLO", "2")
        check("HELLO 2: inapoi la $-1", c.cmd("GET", "nope") == ("nil", None))
        c.close()

        sub = Client(srv)
        sub.cmd("HELLO", "3")
        check("RESP3: confirmarea SUBSCRIBE e push",
              sub.cmd("SUBSCRIBE", "ch") == ("push", [("bulk", b"subscribe"), ("bulk", b"ch"), ("int", 1)]))
        check("RESP3: comenzi normale in modul subscribe", sub.cmd("SET", "s", "1") == ("ok", "OK"))
        check("RESP3: PING normal in modul subscribe", sub.cmd("PING") == ("ok", "PONG"))
        pub = Client(srv)  # publisher RESP2
        check("PUBLISH catre abonat RESP3", pub.cmd("PUBLISH", "ch", "hello") == ("int", 1))
        check("RESP3: mesajul ajunge ca push",
              sub.read_reply() == ("push", [("bulk", b"message"), ("bulk", b"ch"), ("bulk", b"hello")]))
        check("RESP3: UNSUBSCRIBE ca push",
              sub.cmd("UNSUBSCRIBE") == ("push", [("bulk", b"unsubscribe"), ("bulk", b"ch"), ("int", 0)]))
        check("RESP3: UNSUBSCRIBE fara abonamente",
              sub.cmd("UNSUBSCRIBE") == ("push", [("bulk", b"unsubscribe"), ("null", None), ("int", 0)]))
        pub.close()
        sub.close()
    finally:
        srv.cleanup()


def sec_real_clients(check):
    cli, bench = shutil.which("redis-cli"), shutil.which("redis-benchmark")
    if not cli or not bench:
        print("SKIP redis-cli / redis-benchmark nu sunt instalate")
        return
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        port = str(srv.port)
        out = subprocess.run([cli, "-p", port], input=b"SET a 1\nMGET a nope\nEXPIRE a 50\nTTL a\nPING\n",
                             capture_output=True, timeout=20)
        check("redis-cli: sesiune de comenzi", out.returncode == 0 and out.stdout.split() ==
              [b"OK", b"1", b"1", b"50", b"PONG"], out)

        out = subprocess.run([cli, "-3", "-p", port], input=b"SET r3 1\nGET r3\nPING\n",
                             capture_output=True, timeout=20)
        check("redis-cli -3: sesiune RESP3", out.returncode == 0 and out.stdout.split() == [b"OK", b"1", b"PONG"], out)

        out = subprocess.run([bench, "-p", port, "-t", "ping,set,get,mset", "-n", "2000", "-q"],
                             capture_output=True, timeout=120)
        tests = [ln.split(b":")[0] for ln in out.stdout.replace(b"\r", b"\n").splitlines() if b"requests per second" in ln]
        check("redis-benchmark -t ping,set,get,mset", out.returncode == 0 and
              {b"PING_INLINE", b"PING_MBULK", b"SET", b"GET"} <= set(tests) and any(t.startswith(b"MSET") for t in tests),
              (out.returncode, out.stdout[-400:], out.stderr[-400:]))
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_basics, sec_cold_and_restart, sec_pubsub, sec_inline_quotes, sec_expire_options, sec_resp3,
                sec_real_clients):
        run_with_retry(sec, fails)
    report(fails, "s7")


if __name__ == "__main__":
    main()
