"""S1 "excrescencies": cold storage fara inviere, cu metadata si compactare.

Cu SOMNIUM_MAX_KEYS=0 fiecare SET muta in cold storage toate cheile din RAM
ale camerei, deci fiecare pas (evict, reload, overwrite, delete, compact,
restart) e determinist.
"""

import os
import urllib.request

from common import Client, Server, report, run_with_retry

ALL_COLD = {"SOMNIUM_MAX_KEYS": "0"}


def metrics():
    body = urllib.request.urlopen("http://127.0.0.1:9090/metrics", timeout=5).read().decode()
    return {ln.split()[0]: int(ln.split()[1]) for ln in body.splitlines() if ln and not ln.startswith("#")}


def sec_lifecycle(check):
    srv = Server(env_extra=ALL_COLD)  # cu exporterul de metrici pornit
    try:
        srv.start()
        c = Client(srv)

        c.cmd("SET", "k", "v1")
        m = metrics()
        check("SET evict-uit imediat", m["db_keys_evicted"] == 1 and m["db_keys_in_ram"] == 0, m)
        check("reload din cold", c.cmd("GET", "k") == ("bulk", b"v1"))
        c.cmd("SET", "k", "v2")
        check("overwrite dupa reload", c.cmd("GET", "k") == ("bulk", b"v2"))
        c.cmd("SET", "k", "v3")
        check("DEL pe o cheie aflata doar in cold", c.cmd("DEL", "k") == ("int", 1))
        check("fara inviere dupa DEL", c.cmd("GET", "k")[0] == "nil")
        check("al doilea DEL", c.cmd("DEL", "k") == ("int", 0))

        c.cmd("SET", "", "")
        check("cheie goala + valoare goala", c.cmd("GET", "") == ("bulk", b""))
        check("absent != gol", c.cmd("GET", "missing")[0] == "nil")

        for room in ("a", "b"):
            c.cmd("ROOM", room)
            c.cmd("SET", "same", room)
        for room in ("a", "b"):
            c.cmd("ROOM", room)
            check(f"camere independente: {room}", c.cmd("GET", "same") == ("bulk", room.encode()))

        c.cmd("CRDTMERGE", "ck", "cv", "424242", "42")
        c.cmd("SET", "trigger", "1")  # evict-uieste si ck, cu versiunea ei
        r = c.cmd("CRDTMERGE", "ck", "stale", "5", "1")
        check("versiunea CRDT supravietuieste evict + reload", "Ignored" in r[1], r)
        c.cmd("SET", "trigger", "2")  # ck inapoi in cold pentru compactare

        for i in range(1000):
            c.cmd("SET", "hot", f"v{i}")
        before = metrics()
        check("1000 de versiuni lasa octeti obsoleti", before["db_cold_obsolete_bytes"] > 0, before)

        r = c.cmd("COMPACT")
        after = metrics()
        check("COMPACT intoarce octetii recuperati", r[0] == "int" and r[1] == before["db_cold_obsolete_bytes"], (r, before))
        check("dupa compactare: zero obsoleti", after["db_cold_obsolete_bytes"] == 0, after)
        check("metrica de octeti recuperati", after["db_cold_reclaimed_bytes"] == r[1], after)
        check("fisierul s-a micsorat exact",
              after["db_cold_file_bytes"] == before["db_cold_file_bytes"] - r[1] ==
              os.path.getsize(os.path.join(srv.workdir, "cold.bin")), (before, after))
        check("ultima versiune dupa compactare", c.cmd("GET", "hot") == ("bulk", b"v999"))
        r = c.cmd("CRDTMERGE", "ck", "stale", "5", "1")
        check("metadata CRDT dupa compactare", "Ignored" in r[1], r)
        c.close()

        srv.restart()
        c = Client(srv)
        check("restart: DEL respectat", c.cmd("GET", "k")[0] == "nil")
        check("restart: cheie goala", c.cmd("GET", "") == ("bulk", b""))
        c.cmd("ROOM", "a")
        check("restart: camera a", c.cmd("GET", "same") == ("bulk", b"a"))
        c.cmd("ROOM", "b")
        check("restart: ultima versiune", c.cmd("GET", "hot") == ("bulk", b"v999"))
        r = c.cmd("CRDTMERGE", "ck", "stale", "5", "1")
        check("restart: metadata CRDT", "Ignored" in r[1], r)
        check("restart: COMPACT functioneaza", c.cmd("COMPACT")[0] == "int")
        c.close()
    finally:
        srv.cleanup()


def sec_failed_compaction(check):
    srv = Server(env_extra={**ALL_COLD, "SOMNIUM_NO_METRICS": "1"})
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "a", "1")
        c.cmd("SET", "a", "2")  # o versiune obsoleta
        c.cmd("SET", "b", "3")
        tmp = os.path.join(srv.workdir, "cold.bin.tmp")
        os.mkdir(tmp)  # injectie: fisierul temporar nu poate fi creat
        check("compactare esuata -> -ERR", c.cmd("COMPACT")[0] == "err")
        check("fisierul vechi ramane folosibil",
              c.cmd("GET", "a") == ("bulk", b"2") and c.cmd("GET", "b") == ("bulk", b"3"))
        os.rmdir(tmp)
        c.cmd("SET", "c", "4")
        check("compactarea reuseste dupa inlaturarea cauzei", c.cmd("COMPACT")[0] == "int")
        check("datele dupa compactare",
              [c.cmd("GET", k)[1] for k in ("a", "b", "c")] == [b"2", b"3", b"4"])
        c.close()
    finally:
        srv.cleanup()


def sec_failed_eviction(check):
    srv = Server(env_extra={**ALL_COLD, "SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
    try:
        os.mkdir(os.path.join(srv.workdir, "cold.bin"))  # injectie: cold storage indisponibil
        srv.start()
        c = Client(srv)
        check("SET fara cold storage", c.cmd("SET", "k", "v") == ("ok", "OK"))
        check("evictarea esuata lasa cheia in RAM", c.cmd("GET", "k") == ("bulk", b"v"))
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_lifecycle, sec_failed_compaction, sec_failed_eviction):
        run_with_retry(sec, fails)
    report(fails, "s1")


if __name__ == "__main__":
    main()
