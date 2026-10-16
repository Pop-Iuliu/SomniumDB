"""Stabilitate: regresii gasite la testul de stres.

Verifica: o scriere partiala (disc plin) nu lasa o inregistrare rupta nici in AOF
(altfel replay-ul pierde tot ce urmeaza dupa ea), nici in cold storage (altfel
indexul citeste valoarea altei chei). Discul plin e simulat fara root: limita de
marime a fisierelor procesului (RLIMIT_FSIZE, prin prlimit) face ca urmatoarea
scriere mare sa ajunga partial si sa esueze cu EFBIG, iar ridicarea ei e spatiul
eliberat.
"""

import os
import resource

from common import Client, Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1", "SOMNIUM_AOF_SYNC": "no"}
UNLIMITED = resource.RLIM_INFINITY


def limit_file_size(srv, soft):
    resource.prlimit(srv.proc.pid, resource.RLIMIT_FSIZE, (soft, UNLIMITED))


def size(srv, name):
    return os.path.getsize(os.path.join(srv.workdir, name))


def sec_torn_aof(check):
    srv = Server(env_extra=ENV, keep_dir_on_retry=True)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "before", "1")
        limit_file_size(srv, size(srv, "appendonly.aof") + 40000)
        c.cmd("SET", "big", b"x" * 100000)  # ajunge partial in AOF si esueaza
        check("disc plin: SAVE raporteaza eroarea", c.cmd("SAVE")[0] == "err")
        limit_file_size(srv, UNLIMITED)  # spatiul revine
        for i in range(10):
            c.cmd("SET", f"later{i}", f"v{i}")
        check("spatiu eliberat: AOF sanatos din nou", c.cmd("SAVE")[0] == "ok")
        c.close()

        srv.restart()
        c = Client(srv)
        check("restart: scrierea dinainte supravietuieste", c.cmd("GET", "before") == ("bulk", b"1"))
        kept = sum(c.cmd("GET", f"later{i}") == ("bulk", f"v{i}".encode()) for i in range(10))
        check("restart: scrierile de dupa eliberarea spatiului supravietuiesc", kept == 10, f"{kept}/10")
        c.close()
        log = open(os.path.join(srv.workdir, "server.0.log"), "rb").read()
        check("restart: fara coada trunchiata sau AOF corupt",
              b"trunchiata" not in log and b"corupt" not in log, log[-400:])
    finally:
        srv.cleanup()


def sec_torn_cold(check):
    # SOMNIUM_MAX_KEYS=0: dupa fiecare comanda toate cheile camerei pleaca in cold storage,
    # deci GET-urile cresc cold.bin fara sa creasca AOF-ul
    srv = Server(env_extra={**ENV, "SOMNIUM_MAX_KEYS": "0"}, keep_dir_on_retry=True)
    try:
        srv.start()
        c = Client(srv)
        value = lambda i: (f"value-{i:02d}-".encode()) * 4000  # ~40 KB, alta pentru fiecare cheie
        keys = [f"k{i}" for i in range(6)]
        for i, k in enumerate(keys):
            c.cmd("SET", k, value(i))
        for _ in range(3):
            for k in keys:
                c.cmd("GET", k)
        cold = size(srv, "cold.bin")
        check("pregatire: cold.bin mai mare decat AOF-ul", cold > size(srv, "appendonly.aof") + 40000)
        limit_file_size(srv, cold + 20000)  # urmatoarea evictare ajunge partial in cold.bin
        for k in keys[:3]:
            c.cmd("GET", k)
        limit_file_size(srv, UNLIMITED)
        wrong = []
        for _ in range(2):
            for i, k in enumerate(keys):
                if c.cmd("GET", k) != ("bulk", value(i)):
                    wrong.append(k)
        check("dupa o evictare partiala, fiecare cheie isi citeste valoarea", not wrong, wrong)
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_torn_aof, sec_torn_cold):
        run_with_retry(sec, fails)
    report(fails, "stab")


if __name__ == "__main__":
    main()
