"""S2 "sundaes": SET compozabil (NX/XX, EX/PX) si TTL/PTTL.

Verifica validarea stricta fara efecte, conditiile pe starea logica (cold
storage si chei expirate), absenta mutatiilor in AOF la conditii esuate, si
ca termenul absolut supravietuieste evictarii, hibernarii si restartului fara
sa castige viata in plus (ceas controlabil: SOMNIUM_CLOCK_OFFSET_MS).
"""

import os
import time

from common import Client, Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}


def aof_size(srv):
    return os.path.getsize(os.path.join(srv.workdir, "appendonly.aof"))


def sec_validation(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "k", "orig")
        size = aof_size(srv)
        bad = [
            ("NX", "XX"), ("XX", "NX"), ("NX", "NX"), ("EX", "10", "PX", "100"), ("EX",), ("PX",),
            ("EX", "0"), ("EX", "-5"), ("PX", "abc"), ("PX", "+5"), ("EX", "99999999999999999999"),
            ("EX", "9223372036854775"), ("FOO",), ("EX", "10", "EX", "10"),
        ]
        for opts in bad:
            r = c.cmd("SET", "k", "changed", *opts)
            check(f"SET {' '.join(opts)} -> -ERR", r[0] == "err", r)
        check("datele neatinse", c.cmd("GET", "k") == ("bulk", b"orig"))
        check("TTL neatins", c.cmd("PTTL", "k") == ("int", -1))
        check("nimic in AOF", aof_size(srv) == size)
        check("optiunile nu tin cont de majuscule", c.cmd("SET", "k", "v", "nx")[0] == "nil")
        check("wrong args TTL", c.cmd("TTL")[0] == "err")
        c.close()
    finally:
        srv.cleanup()


def sec_conditions(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "seed", "1")  # AOF creat
        size = aof_size(srv)
        check("XX pe cheie absenta -> nil", c.cmd("SET", "nk", "v", "XX") == ("nil", None))
        check("XX esuat nu creeaza cheia", c.cmd("GET", "nk")[0] == "nil")
        check("conditia esuata nu scrie in AOF", aof_size(srv) == size)
        check("NX pe cheie absenta", c.cmd("SET", "nk", "v1", "NX") == ("ok", "OK"))
        size = aof_size(srv)
        check("NX pe cheie existenta -> nil", c.cmd("SET", "nk", "v2", "NX") == ("nil", None))
        check("NX esuat nu scrie in AOF", aof_size(srv) == size)
        check("XX pe cheie existenta", c.cmd("SET", "nk", "v3", "XX") == ("ok", "OK"))
        check("valoarea dupa XX", c.cmd("GET", "nk") == ("bulk", b"v3"))

        check("TTL cheie absenta", c.cmd("TTL", "nope") == ("int", -2))
        check("TTL cheie persistenta", c.cmd("TTL", "nk") == ("int", -1))
        c.cmd("SET", "t", "v", "EX", "100")
        check("TTL dupa EX 100", c.cmd("TTL", "t") == ("int", 100))
        p = c.cmd("PTTL", "t")[1]
        check("PTTL dupa EX 100", 99000 < p <= 100000, p)
        c.cmd("SET", "t", "v", "NX", "PX", "5000")  # NX esuat: TTL-ul ramane
        check("NX esuat nu schimba TTL-ul", c.cmd("TTL", "t") == ("int", 100))
        c.cmd("SET", "t", "v2")
        check("SET simplu sterge TTL-ul", c.cmd("TTL", "t") == ("int", -1))

        c.cmd("SET", "e", "v", "PX", "200")
        time.sleep(0.4)
        check("cheie expirata: GET nil", c.cmd("GET", "e")[0] == "nil")
        check("cheie expirata: TTL -2", c.cmd("TTL", "e") == ("int", -2))
        check("cheie expirata conteaza ca absenta pentru NX", c.cmd("SET", "e", "v2", "NX") == ("ok", "OK"))
        c.cmd("SET", "e2", "v", "PX", "100")
        time.sleep(0.3)
        check("XX pe cheie expirata -> nil", c.cmd("SET", "e2", "v", "XX") == ("nil", None))
        c.close()
    finally:
        srv.cleanup()


def sec_transitions(check):
    # toate cheile ajung in cold storage dupa fiecare SET
    srv = Server(env_extra={**ENV, "SOMNIUM_MAX_KEYS": "0"})
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "c", "v", "PX", "60000")
        p1 = c.cmd("PTTL", "c")[1]  # reincarcata din cold
        check("termenul supravietuieste evictarii", 55000 < p1 <= 60000, p1)
        c.cmd("SET", "other", "x")  # c inapoi in cold
        p2 = c.cmd("PTTL", "c")[1]
        check("fara viata in plus dupa evict + reload", 0 < p2 <= p1, (p1, p2))
        check("NX vede cheile din cold", c.cmd("SET", "c", "v2", "NX") == ("nil", None))

        c.cmd("SET", "d", "v", "PX", "300")
        c.cmd("SET", "other", "y")
        time.sleep(0.5)
        check("expirata in cold: absenta", c.cmd("GET", "d")[0] == "nil")
        check("XX pe cheie expirata in cold", c.cmd("SET", "d", "v", "XX") == ("nil", None))

        c.cmd("ROOM", "h")
        c.cmd("SET", "x", "v", "PX", "60000")
        p1 = c.cmd("PTTL", "x")[1]  # reincarcarea o aduce in RAM, deci intra in snapshot
        check("ROOM.HIBERNATE", c.cmd("ROOM.HIBERNATE", "h") == ("ok", "OK"))
        p2 = c.cmd("PTTL", "x")[1]  # trezire din snapshot
        check("fara viata in plus dupa hibernare", 0 < p2 <= p1, (p1, p2))
        c.close()
    finally:
        srv.cleanup()


def sec_restart_clock(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "short", "v", "PX", "10000")
        c.cmd("SET", "long", "v", "PX", "30000")
        c.cmd("SET", "forever", "v")
        p = c.cmd("PTTL", "long")[1]
        # merge mai vechi decat un SET expirat: live cheia e absenta, deci converge
        c.cmd("SET", "em", "old", "PX", "100")
        time.sleep(0.3)
        r = c.cmd("CRDTMERGE", "em", "merged", "5", "1")
        check("CRDTMERGE peste cheie expirata converge", "Converged" in r[1], r)
        c.close()

        # 20s "trec" cat serverul e oprit
        srv.env_extra["SOMNIUM_CLOCK_OFFSET_MS"] = "20000"
        srv.restart()
        c = Client(srv)
        check("restart: termen depasit in timpul opririi", c.cmd("TTL", "short") == ("int", -2))
        p2 = c.cmd("PTTL", "long")[1]
        check("restart: termen absolut, fara viata in plus", 0 < p2 <= p - 20000, (p, p2))
        check("restart: cheia persistenta", c.cmd("TTL", "forever") == ("int", -1))
        check("restart: NX pe cheia expirata", c.cmd("SET", "short", "v2", "NX") == ("ok", "OK"))
        check("restart: merge-ul redat verbatim", c.cmd("GET", "em") == ("bulk", b"merged"))
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_validation, sec_conditions, sec_transitions, sec_restart_clock):
        run_with_retry(sec, fails)
    report(fails, "s2")


if __name__ == "__main__":
    main()
