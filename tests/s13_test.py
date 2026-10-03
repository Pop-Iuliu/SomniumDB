"""S13 "eldermen": stergeri versionate care converg si se uita in siguranta.

Verifica: DEL, EXPIRE si PERSIST se replica; o scriere mai veche sosita tarziu
nu invie o cheie stearsa, nici inainte, nici dupa ce tombstone-ul a fost uitat
(watermark); tombstone-urile se uita dupa ce frontierele tuturor peer-ilor
le-au depasit si se pastreaza cat timp un peer e oprit.
"""

import re
import time

from common import Client, Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}
PORT_A, PORT_B = 6391, 6392


def node(node_id, port, peer_port):
    return Server(port=port, keep_dir_on_retry=True, env_extra={
        **ENV, "SOMNIUM_NODE_ID": str(node_id), "SOMNIUM_PEERS": f"127.0.0.1:{peer_port}"})


def wait_until(cond, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.2)
    return cond()


def tombstones(client):
    return int(re.search(r"Tombstone-uri: (\d+)", client.cmd("INFO")[1].decode()).group(1))


def old_ts():
    return str(int(time.time() * 1000) - 60000)  # cu un minut in urma


def sec_converge(check):
    a, b = node(1, PORT_A, PORT_B), node(2, PORT_B, PORT_A)
    try:
        a.start()
        b.start()
        ca, cb = Client(a), Client(b)

        ca.cmd("SET", "k", "v")
        check("SET ajunge pe B", wait_until(lambda: cb.cmd("GET", "k") == ("bulk", b"v")))
        check("DEL", ca.cmd("DEL", "k") == ("int", 1))
        check("DEL se replica", wait_until(lambda: cb.cmd("GET", "k")[0] == "nil"))

        ca.cmd("SET", "e", "v")
        check("EXPIRE", ca.cmd("EXPIRE", "e", "100") == ("int", 1))
        check("EXPIRE se replica", wait_until(lambda: cb.cmd("TTL", "e")[1] in (99, 100)), cb.cmd("TTL", "e"))
        check("PERSIST", ca.cmd("PERSIST", "e") == ("int", 1))
        check("PERSIST se replica", wait_until(lambda: cb.cmd("TTL", "e") == ("int", -1)))

        ca.cmd("SET", "r", "v1")
        check("r ajunge pe B", wait_until(lambda: cb.cmd("GET", "r") == ("bulk", b"v1")))
        ca.cmd("DEL", "r")
        check("tombstone pe A", tombstones(ca) >= 1)
        check("DEL r ajunge pe B", wait_until(lambda: cb.cmd("GET", "r")[0] == "nil"))
        r = cb.cmd("CRDTMERGE", "r", "zombie", old_ts(), "9")
        check("o scriere veche nu invie cheia (tombstone)", "Ignored" in r[1], r)

        check("tombstone-urile se uita dupa frontiere",
              wait_until(lambda: tombstones(ca) == 0 and tombstones(cb) == 0), (tombstones(ca), tombstones(cb)))
        r = ca.cmd("CRDTMERGE", "r", "zombie", old_ts(), "9")
        check("nici dupa uitare (watermark)", "Ignored" in r[1], r)
        check("cheia ramane stearsa", ca.cmd("GET", "r")[0] == "nil" and cb.cmd("GET", "r")[0] == "nil")
        ca.close()
        cb.close()
    finally:
        a.cleanup()
        b.cleanup()


def sec_peer_down(check):
    a, b = node(1, PORT_A, PORT_B), node(2, PORT_B, PORT_A)
    try:
        a.start()
        b.start()
        ca = Client(a)
        ca.cmd("SET", "x", "1")
        b.stop()
        ca.cmd("DEL", "x")
        time.sleep(3)
        check("tombstone pastrat cat timp peer-ul e oprit", tombstones(ca) == 1, tombstones(ca))

        b.wait_port_free()
        b.start()
        cb = Client(b)
        check("dupa revenire: stergerea ajunge si tombstone-urile se uita",
              wait_until(lambda: cb.cmd("GET", "x")[0] == "nil" and tombstones(ca) == 0 and tombstones(cb) == 0),
              (tombstones(ca), tombstones(cb)))
        ca.close()
        cb.close()
    finally:
        a.cleanup()
        b.cleanup()


def main():
    fails = []
    for sec in (sec_converge, sec_peer_down):
        run_with_retry(sec, fails)
    report(fails, "s13")


if __name__ == "__main__":
    main()
