"""S7 "catholicized": orice client Redis e binevenit.

Verifica: comenzi inline, PING/ECHO/QUIT, EXISTS/MGET/MSET (cold storage
inclus, expiratele absente), EXPIRE/PEXPIRE/PERSIST cu termen absolut peste
restart, SUBSCRIBE multiplu + UNSUBSCRIBE + PING in modul subscribe, si
redis-cli / redis-benchmark reale cand sunt instalate (in CI sunt).
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
    for sec in (sec_basics, sec_cold_and_restart, sec_pubsub, sec_real_clients):
        run_with_retry(sec, fails)
    report(fails, "s7")


if __name__ == "__main__":
    main()
