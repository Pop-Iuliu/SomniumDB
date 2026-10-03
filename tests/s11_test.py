"""S11 "glidder": versiuni HLC in loc de ceasul de perete.

Verifica: doua noduri cu ceasurile la 20s distanta converg cand nodul cu
ceasul in urma suprascrie o cheie (inainte, versiunea scadea si peer-ul ignora
scrierea: divergenta permanenta); o versiune prea departe in viitor e
respinsa; scrierile locale depasesc versiunea suprascrisa si orice versiune
vazuta, inclusiv dupa restart.
"""

import time

from common import Client, Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}
PORT_A, PORT_B = 6391, 6392


def wait_until(cond, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.2)
    return cond()


def now_ms():
    return int(time.time() * 1000)


def sec_skew(check):
    a = Server(port=PORT_A, keep_dir_on_retry=True, env_extra={
        **ENV, "SOMNIUM_NODE_ID": "1", "SOMNIUM_PEERS": f"127.0.0.1:{PORT_B}"})
    b = Server(port=PORT_B, keep_dir_on_retry=True, env_extra={  # ceasul lui B e 20s inainte
        **ENV, "SOMNIUM_NODE_ID": "2", "SOMNIUM_PEERS": f"127.0.0.1:{PORT_A}", "SOMNIUM_CLOCK_OFFSET_MS": "20000"})
    try:
        a.start()
        b.start()
        ca, cb = Client(a), Client(b)
        cb.cmd("SET", "k", "from-b")
        check("scrierea lui B ajunge pe A", wait_until(lambda: ca.cmd("GET", "k") == ("bulk", b"from-b")))
        ca.cmd("SET", "k", "from-a")  # ceasul lui A e in urma versiunii stocate
        check("scrierea ulterioara a lui A castiga pe ambele noduri",
              wait_until(lambda: cb.cmd("GET", "k") == ("bulk", b"from-a")), cb.cmd("GET", "k"))
        check("A pastreaza propria scriere", ca.cmd("GET", "k") == ("bulk", b"from-a"))
        ca.close()
        cb.close()
    finally:
        a.cleanup()
        b.cleanup()


def sec_versions(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        check("versiune absurda respinsa", c.cmd("CRDTMERGE", "k", "v", "18446744073709551615", "1")[0] == "err")
        check("versiune cu o ora in viitor respinsa", c.cmd("CRDTMERGE", "k", "v", str(now_ms() + 3600000), "1")[0] == "err")
        r = c.cmd("CRDTMERGE", "k", "remote", str(now_ms() + 5000), "9")
        check("versiune cu 5s in viitor acceptata", "Converged" in r[1], r)
        c.cmd("SET", "k", "local")
        r = c.cmd("CRDTMERGE", "k", "again", str(now_ms() + 5000), "9")
        check("SET-ul local depaseste versiunea suprascrisa", "Ignored" in r[1], r)
        c.cmd("CRDTMERGE", "z", "far", str(now_ms() + 60000), "9")  # ceasul observa +60s
        c.close()

        srv.restart()
        c = Client(srv)
        r = c.cmd("CRDTMERGE", "k", "late", str(now_ms() + 5000), "9")
        check("versiunea redata dupa restart", "Ignored" in r[1], r)
        c.cmd("SET", "w", "fresh")  # cheie noua: versiunea vine din ceasul observat la replay
        r = c.cmd("CRDTMERGE", "w", "older", str(now_ms() + 30000), "9")
        check("dupa restart, scrierile locale depasesc versiunile vazute", "Ignored" in r[1], r)
        check("valoarea locala ramane", c.cmd("GET", "w") == ("bulk", b"fresh"))
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_skew, sec_versions):
        run_with_retry(sec, fails)
    report(fails, "s11")


if __name__ == "__main__":
    main()
