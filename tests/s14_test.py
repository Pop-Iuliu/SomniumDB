"""S14 "padnags": rescrierea AOF in fundal, fara pauza pentru clienti.

Verifica: in timpul rescrierii unui set mare de date, latenta clientilor ramane
mica (instantaneul copy-on-write se scrie intr-un copil al fork-ului); scrierile
si stergerile facute cat timp copilul scrie ajung in noul AOF prin coada de la
fork si supravietuiesc restartului; o a doua cerere in timpul rescrierii e
refuzata clar.
"""

import os
import time

from common import Client, Server, report, resp_cmd, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}
KEYS = 200000


def rewriting(c):
    return b"Rescriere AOF: in curs" in c.cmd("INFO")[1]


def sec_background(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        value = "v" * 100
        batch = 1000
        commands = [resp_cmd("MSET", *[x for i in range(start, start + batch) for x in (f"k{i}", value)])
                    for start in range(0, KEYS, batch)]
        c.s.sendall(b"".join(commands))
        check("set mare de date incarcat", all(c.read_reply() == ("ok", "OK") for _ in commands))

        t0 = time.time()
        check("REWRITEAOF porneste in fundal", c.cmd("REWRITEAOF")[0] == "ok")
        r = c.cmd("REWRITEAOF")
        check("a doua cerere in timpul rescrierii e refuzata", r[0] == "err" or not rewriting(c), r)

        worst, i = 0.0, 0
        while True:
            t = time.time()
            c.cmd("SET", f"during{i}", str(i))  # ajunge in coada de la fork
            if i % 2 == 0:
                c.cmd("DEL", f"k{i}")
            worst = max(worst, time.time() - t)
            i += 1
            if i % 20 == 0 and not rewriting(c):
                break
            if time.time() - t0 > 60:
                break
        took = time.time() - t0
        print(f"  rescriere: {took * 1000:.0f} ms, {i} scrieri in paralel, latenta maxima {worst * 1000:.1f} ms")
        check("rescrierea s-a terminat", not rewriting(c))
        check("latenta clientilor ramane mica in timpul rescrierii", worst < 0.15, f"{worst * 1000:.1f} ms")
        check("fara fisier temporar ramas", not os.path.exists(os.path.join(srv.workdir, "appendonly.aof.tmp")))
        c.close()

        srv.restart()
        c = Client(srv)
        check("scrierile din timpul rescrierii supravietuiesc",
              all(c.cmd("GET", f"during{j}") == ("bulk", str(j).encode()) for j in range(0, i, max(1, i // 50))))
        check("stergerile din timpul rescrierii supravietuiesc",
              all(c.cmd("EXISTS", f"k{j}") == ("int", 0) for j in range(0, i, 2)))
        check("restul datelor intact", c.cmd("GET", f"k{KEYS - 1}") == ("bulk", value.encode()))
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    run_with_retry(sec_background, fails)
    report(fails, "s14")


if __name__ == "__main__":
    main()
