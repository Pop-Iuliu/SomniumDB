"""S9 "scolog": camerele platesc chirie in octeti (SOMNIUM_MAXMEMORY).

Verifica: scrierile peste buget tin octetii rezidenti sub buget plus o
inregistrare, iar toate cheile raman citibile; contoarele revin la zero dupa
stergere, expirare, evictare si hibernare; o camera prea mare pentru bugetul
liber nu se trezeste si nu schimba nimic.
"""

import random
import time

from common import Client, Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}


def resident(c):
    info = c.cmd("INFO")[1].decode()
    return int(info.split("Octeti rezidenti: ")[1].split("\n")[0])


def room_info(c, room):
    items = [v for _, v in c.cmd("ROOM.INFO", room)[1]]
    return {items[i].decode(): items[i + 1] for i in range(0, len(items), 2)}


def sec_budget(check):
    budget, biggest = 20000, 2000
    srv = Server(env_extra={**ENV, "SOMNIUM_MAXMEMORY": str(budget)})
    try:
        srv.start()
        c = Client(srv)
        rng = random.Random(7)
        values = {f"k{i}": "v" * rng.randint(10, biggest) for i in range(200)}
        peak = 0
        for key, value in values.items():
            c.cmd("SET", key, value)
            peak = max(peak, resident(c))
        # o inregistrare: cheia, cea mai mare valoare si regia fixa (sub 256 de octeti)
        check("octetii rezidenti raman sub buget plus o inregistrare", peak <= budget + biggest + 256, peak)
        got = c.cmd("MGET", *values)[1]
        check("toate cheile raman citibile", got == [("bulk", v.encode()) for v in values.values()])
        check("si dupa citiri, sub buget", resident(c) <= budget + biggest + 256, resident(c))
        c.close()
    finally:
        srv.cleanup()


def sec_no_drift(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("MSET", "a", "1" * 100, "b", "2" * 50)
        c.cmd("SET", "a", "x" * 10)  # valoarea se micsoreaza
        c.cmd("CRDTMERGE", "m", "y" * 30, "424242", "4")
        check("octetii cresc cu datele", resident(c) > 0)
        check("ROOM.INFO raporteaza octetii camerei", room_info(c, "default").get("bytes") == resident(c))
        for key in ("a", "b", "m"):
            c.cmd("DEL", key)
        c.cmd("SET", "e", "v", "PX", "100")
        time.sleep(0.3)
        c.cmd("GET", "e")  # expirarea o scoate
        check("zero dupa stergere si expirare", resident(c) == 0, resident(c))

        c.cmd("ROOM", "h")
        c.cmd("MSET", "x", "1" * 500, "y", "2" * 500)
        check("ROOM.HIBERNATE", c.cmd("ROOM.HIBERNATE", "h") == ("ok", "OK"))
        check("zero dupa hibernare", resident(c) == 0, resident(c))
        c.cmd("ROOM", "h")
        check("trezirea taxeaza din nou camera", resident(c) == room_info(c, "h").get("bytes") > 1000)
        c.close()
    finally:
        srv.cleanup()

    srv = Server(env_extra={**ENV, "SOMNIUM_MAX_KEYS": "0"})  # totul ajunge in cold storage
    try:
        srv.start()
        c = Client(srv)
        c.cmd("MSET", "a", "1" * 100, "b", "2" * 100)
        check("zero dupa evictare", resident(c) == 0, resident(c))
        c.close()
    finally:
        srv.cleanup()


def sec_wake_too_big(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_MAXMEMORY": "5000"})
    try:
        srv.start()
        c = Client(srv)
        c.cmd("ROOM", "big")
        c.cmd("MSET", "a", "A" * 1000, "b", "B" * 1000, "c", "C" * 1000)
        c.cmd("ROOM.HIBERNATE", "big")
        c.cmd("ROOM", "small")
        c.cmd("SET", "s", "S" * 2000)
        before = resident(c)

        check("camera prea mare nu se trezeste", c.cmd("ROOM", "big")[0] == "err")
        check("ramane adormita", room_info(c, "big").get("state") == b"sleeping")
        check("octetii nu s-au schimbat", resident(c) == before, (before, resident(c)))
        check("camera activa e intacta", c.cmd("GET", "s") == ("bulk", b"S" * 2000))

        c.cmd("ROOM.HIBERNATE", "small")
        check("dupa eliberarea bugetului se trezeste", c.cmd("ROOM", "big") == ("ok", "OK"))
        check("datele camerei mari intacte", c.cmd("MGET", "a", "b", "c")[1] ==
              [("bulk", b"A" * 1000), ("bulk", b"B" * 1000), ("bulk", b"C" * 1000)])
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_budget, sec_no_drift, sec_wake_too_big):
        run_with_retry(sec, fails)
    report(fails, "s9")


if __name__ == "__main__":
    main()
