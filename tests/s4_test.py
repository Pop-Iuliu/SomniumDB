"""S4 "rezoned": ciclul de viata al camerelor si bugetul de camere active.

Verifica: bugetul de 3 camere la creare si la trezire, izolarea datelor prin
snapshot-uri, ROOMS / ROOM.INFO / ROOM.HIBERNATE / ROOM.WAKE, un esec de
snapshot care lasa camera activa, si un stres marginit in care comenzile,
INFO si inspectia ruleaza in timp ce watchdog-ul hiberneaza camere (CI il
ruleaza si sub ThreadSanitizer).
"""

import os
import time

from common import Client, Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}


def info(c, room):
    r = c.cmd("ROOM.INFO", room)
    if r[0] != "array":
        return {}
    items = [v for _, v in r[1]]
    return {items[i].decode(): items[i + 1] for i in range(0, len(items), 2)}


def states(c):
    names = [n.decode() for _, n in c.cmd("ROOMS")[1]]
    return {n: info(c, n)["state"] for n in names}


def active_count(c):
    return sum(s == b"active" for s in states(c).values())


def sec_budget(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        rooms = [f"r{i}" for i in range(6)]
        over = False

        for i, room in enumerate(rooms):
            if i >= 3:
                check(f"buget plin: ROOM {room} refuzat", c.cmd("ROOM", room)[0] == "err")
                check(f"ROOM.HIBERNATE {rooms[i - 3]}", c.cmd("ROOM.HIBERNATE", rooms[i - 3]) == ("ok", "OK"))
            check(f"ROOM {room}", c.cmd("ROOM", room) == ("ok", "OK"))
            c.cmd("SET", "k", room)
            over |= active_count(c) > 3

        # trezirea trece prin acelasi buget: eliberam mereu camera de acum 3 pasi
        isolated = True
        for i, room in enumerate(rooms):
            c.cmd("ROOM.HIBERNATE", rooms[(i + 3) % 6])
            isolated &= c.cmd("ROOM", room) == ("ok", "OK") and c.cmd("GET", "k") == ("bulk", room.encode())
            over |= active_count(c) > 3
        check("sase camere prin snapshot-uri: date izolate", isolated)
        check("nicio activare peste bugetul de 3", not over)

        # aici sunt active r3, r4, r5
        check("ROOM.WAKE refuzat cand bugetul e plin", c.cmd("ROOM.WAKE", "r0")[0] == "err")
        c.cmd("ROOM.HIBERNATE", "r3")
        check("ROOM.WAKE", c.cmd("ROOM.WAKE", "r0") == ("ok", "OK"))
        r0 = info(c, "r0")
        check("ROOM.INFO activa: stare si chei", r0.get("state") == b"active" and r0.get("keys") == 1, r0)
        check("ROOM.INFO: ultimul acces", r0.get("last_access_ms", 0) > 0, r0)
        r3 = info(c, "r3")
        check("ROOM.INFO adormita", r3.get("state") == b"sleeping" and r3.get("keys") == 0, r3)
        check("ROOM.INFO camera inexistenta", c.cmd("ROOM.INFO", "nope")[0] == "err")
        check("ROOM.HIBERNATE camera inexistenta", c.cmd("ROOM.HIBERNATE", "nope")[0] == "err")
        check("default e fixata", c.cmd("ROOM.HIBERNATE", "default")[0] == "err")

        c.cmd("ROOM.HIBERNATE", "r4")
        check("o camera numita LIST e valida", c.cmd("ROOM", "LIST") == ("ok", "OK"))
        c.cmd("SET", "x", "1")
        check("SET/GET in camera LIST", c.cmd("GET", "x") == ("bulk", b"1"))
        listed = set(states(c))
        check("ROOMS listeaza toate camerele", {"LIST", *rooms} <= listed, listed)
        c.close()
    finally:
        srv.cleanup()


def sec_failed_snapshot(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("ROOM", "wf")
        c.cmd("SET", "wk", "wv")
        os.mkdir(os.path.join(srv.workdir, "room_wf.bin.tmp"))  # injectie: open() esueaza
        check("ROOM.HIBERNATE cu snapshot esuat -> -ERR", c.cmd("ROOM.HIBERNATE", "wf")[0] == "err")
        check("camera ramane activa", info(c, "wf").get("state") == b"active")
        check("inregistrarile raman citibile", c.cmd("GET", "wk") == ("bulk", b"wv"))
        # locul nu s-a eliberat: wf + a + b umplu bugetul
        c.cmd("ROOM", "a")
        c.cmd("ROOM", "b")
        check("bugetul numara in continuare camera esuata", c.cmd("ROOM", "c")[0] == "err")
        c.close()
    finally:
        srv.cleanup()


def sec_stress(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        for room in ("idle1", "idle2"):
            c.cmd("ROOM", room)
            c.cmd("SET", "k", room)
        c.cmd("ROOM", "busy")

        # watchdog-ul (tick 5s, prag 10s) hiberneaza camerele inactive in timp
        # ce thread-ul de comenzi scrie, citeste si inspecteaza fara pauza
        deadline = time.time() + 25
        i = 0
        while time.time() < deadline:
            c.cmd("SET", f"b{i % 100}", str(i))
            c.cmd("GET", f"b{i % 100}")
            c.cmd("INFO")
            st = states(c)
            i += 1
            if st.get("idle1") == st.get("idle2") == b"sleeping":
                break
        print(f"  {i} iteratii de stres")
        check("watchdog-ul a hibernat camerele inactive",
              st.get("idle1") == st.get("idle2") == b"sleeping", st)
        check("camera folosita a ramas activa", st.get("busy") == b"active", st)

        c.cmd("ROOM", "empty")
        c.cmd("ROOM.HIBERNATE", "empty")
        c.cmd("SET", "late", "write")
        check("scriere dupa hibernarea unei camere goale: accesibila", c.cmd("GET", "late") == ("bulk", b"write"))
        check("camera goala ramane in registru", "empty" in states(c))

        c.cmd("ROOM.HIBERNATE", "empty")
        for room in ("idle1", "idle2"):
            c.cmd("ROOM", room)
            check(f"{room} intacta dupa hibernare", c.cmd("GET", "k") == ("bulk", room.encode()))
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_budget, sec_failed_snapshot, sec_stress):
        run_with_retry(sec, fails)
    report(fails, "s4")


if __name__ == "__main__":
    main()
