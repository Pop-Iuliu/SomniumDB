"""S8 "rebag": AOF-ul rescris ca stare curenta.

Verifica: 10.000 de suprascrieri ajung la o inregistrare per cheie, cu
valori, termene si versiuni CRDT identice dupa restart; cheile expirate nu se
scriu; cheile din cold storage si camerele adormite supravietuiesc; un
snapshot ramas de la o trezire nu invie chei sterse; un esec lasa AOF-ul vechi
in uz; rescrierea automata tine fisierul mic.
"""

import os
import time

from common import Client, Server, report, resp_cmd, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}


def aof(srv):
    with open(os.path.join(srv.workdir, "appendonly.aof"), "rb") as f:
        return f.read()


def records(srv):
    return aof(srv).count(b"SOMNIUM-META")


def wait_rewrite(c, timeout=30):
    """REWRITEAOF ruleaza in fundal (S14); INFO incheie rescrierea cand copilul a terminat."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if b"Rescriere AOF: inactiva" in c.cmd("INFO")[1]:
            return True
        time.sleep(0.05)
    return False


def sec_rewrite_and_restart(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.s.sendall(b"".join(resp_cmd("SET", f"k{i % 100}", f"v{i}") for i in range(10000)))
        check("10.000 de SET-uri", all(c.read_reply() == ("ok", "OK") for _ in range(10000)))
        c.cmd("SET", "ttl", "v", "PX", "60000")
        c.cmd("CRDTMERGE", "ck", "cv", "424242", "42")
        c.cmd("SET", "gone", "v", "PX", "100")
        time.sleep(0.3)
        before = records(srv)
        p = c.cmd("PTTL", "ttl")[1]

        check("REWRITEAOF", c.cmd("REWRITEAOF")[0] == "ok" and wait_rewrite(c))
        check("o inregistrare per cheie vie (fara cea expirata)", records(srv) == 102, (before, records(srv)))
        check("headerul v3 pastrat", aof(srv).startswith(b"*2\r\n$11\r\nSOMNIUM-AOF\r\n$1\r\n3\r\n"))
        check("fara fisier temporar ramas", not os.path.exists(os.path.join(srv.workdir, "appendonly.aof.tmp")))
        c.cmd("SET", "after", "rewrite")  # appendurile continua in fisierul nou
        c.close()

        srv.restart()
        c = Client(srv)
        check("ultimele valori dupa restart",
              c.cmd("MGET", *[f"k{i}" for i in range(100)])[1] == [("bulk", f"v{9900 + i}".encode()) for i in range(100)])
        p2 = c.cmd("PTTL", "ttl")[1]
        check("termenul absolut pastrat", 0 < p2 <= p, (p, p2))
        r = c.cmd("CRDTMERGE", "ck", "stale", "5", "1")
        check("versiunea CRDT pastrata", "Ignored" in r[1], r)
        check("cheia expirata nu a fost scrisa", c.cmd("EXISTS", "gone") == ("int", 0))
        check("appendul de dupa rescriere", c.cmd("GET", "after") == ("bulk", b"rewrite"))
        c.close()
    finally:
        srv.cleanup()


def sec_cold_sleeping_stale(check):
    # o cheie pe camera incape in RAM; a doua muta una dintre ele in cold storage
    srv = Server(env_extra={**ENV, "SOMNIUM_MAX_KEYS": "1"})
    try:
        srv.start()
        c = Client(srv)
        c.cmd("ROOM", "a")
        c.cmd("MSET", "k1", "v1", "k1b", "v1b")

        c.cmd("ROOM", "b")
        c.cmd("SET", "k2", "v2")  # in RAM, deci intra in snapshot
        c.cmd("ROOM.HIBERNATE", "b")

        c.cmd("ROOM", "c")
        c.cmd("SET", "x", "1")
        c.cmd("ROOM.HIBERNATE", "c")
        c.cmd("ROOM", "c")  # trezire: snapshot-ul ramane pe disc, dar se va invechi
        check("DEL dupa trezire", c.cmd("DEL", "x") == ("int", 1))

        check("REWRITEAOF", c.cmd("REWRITEAOF")[0] == "ok" and wait_rewrite(c))
        check("snapshot-ul camerei active sters", not os.path.exists(os.path.join(srv.workdir, "room_c.bin")))
        check("snapshot-ul camerei adormite pastrat", os.path.exists(os.path.join(srv.workdir, "room_b.bin")))
        c.close()

        srv.restart()
        c = Client(srv)
        c.cmd("ROOM", "a")
        check("cheile din RAM si din cold storage supravietuiesc",
              c.cmd("MGET", "k1", "k1b") == ("array", [("bulk", b"v1"), ("bulk", b"v1b")]))
        c.cmd("ROOM", "b")
        check("camera adormita supravietuieste", c.cmd("GET", "k2") == ("bulk", b"v2"))
        c.cmd("ROOM", "c")
        check("cheia stearsa nu invie din snapshot-ul vechi", c.cmd("GET", "x")[0] == "nil")
        c.close()
    finally:
        srv.cleanup()


def sec_failure(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "a", "1")
        tmp = os.path.join(srv.workdir, "appendonly.aof.tmp")
        os.mkdir(tmp)  # injectie: fisierul temporar nu poate fi creat
        check("rescriere esuata -> -ERR", c.cmd("REWRITEAOF")[0] == "err")
        os.rmdir(tmp)
        c.cmd("SET", "b", "2")
        c.close()

        srv.restart()
        c = Client(srv)
        check("AOF-ul vechi a ramas in uz", c.cmd("MGET", "a", "b") == ("array", [("bulk", b"1"), ("bulk", b"2")]))
        c.close()
    finally:
        srv.cleanup()


def sec_auto(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_AOF_REWRITE_MIN_BYTES": "2000"})
    try:
        srv.start()
        c = Client(srv)
        value = "x" * 100
        for i in range(200):  # ~30KB de istoric pentru o singura cheie
            c.cmd("SET", "hot", f"{i}{value}")
        wait_rewrite(c)
        size = len(aof(srv))
        check("rescrierea automata tine AOF-ul mic", size < 4000, size)
        c.close()

        srv.restart()
        c = Client(srv)
        check("valoarea dupa rescrieri automate", c.cmd("GET", "hot") == ("bulk", f"199{value}".encode()))
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_rewrite_and_restart, sec_cold_sleeping_stale, sec_failure, sec_auto):
        run_with_retry(sec, fails)
    report(fails, "s8")


if __name__ == "__main__":
    main()
