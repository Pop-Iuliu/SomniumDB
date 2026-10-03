"""S10 "koreish": caravane de CRDTMERGE intre doua noduri.

Verifica: scrieri concurente pe acelasi set de chei converg la valori
identice; camerele si termenele se replica; nimic nu se intoarce in ecou; un
peer oprit in timpul scrierilor converge dupa repornire; replicarea continua
dupa o rescriere a AOF-ului (S8). DEL ramane local: limita asumata, testata
explicit ca sa nu se schimbe pe nesimtite.
"""

import os
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


def aof(srv):
    with open(os.path.join(srv.workdir, "appendonly.aof"), "rb") as f:
        return f.read()


def sec_converge(check):
    a, b = node(1, PORT_A, PORT_B), node(2, PORT_B, PORT_A)
    try:
        a.start()
        b.start()
        ca, cb = Client(a), Client(b)
        keys = [f"k{i}" for i in range(50)]
        for i, key in enumerate(keys):  # aceleasi chei, scrise concurent pe ambele noduri
            ca.cmd("SET", key, f"a{i}")
            cb.cmd("SET", key, f"b{i}")
        ca.cmd("SET", "only_a", "1")
        ca.cmd("SET", "ttl", "v", "PX", "60000")
        ca.cmd("ROOM", "r")
        ca.cmd("SET", "x", "in-r")
        ca.cmd("ROOM", "default")

        same = lambda: ca.cmd("MGET", *keys) == cb.cmd("MGET", *keys)
        check("scrierile concurente converg", wait_until(same))
        values = ca.cmd("MGET", *keys)[1]
        check("fiecare valoare vine de la unul dintre noduri",
              all(v[1] in (f"a{i}".encode(), f"b{i}".encode()) for i, v in enumerate(values)))
        check("cheia scrisa doar pe A ajunge pe B", wait_until(lambda: cb.cmd("GET", "only_a") == ("bulk", b"1")))
        cb.cmd("ROOM", "r")
        check("camera se replica", wait_until(lambda: cb.cmd("GET", "x") == ("bulk", b"in-r")))
        cb.cmd("ROOM", "default")
        p = cb.cmd("PTTL", "ttl")[1]
        check("termenul se replica", 0 < p <= 60000, p)

        time.sleep(1.5)  # inca doua caravane: un ecou ar fi aparut deja
        check("fara ecou: o singura inregistrare pe fiecare nod",
              aof(a).count(b"only_a") == 1 and aof(b).count(b"only_a") == 1)

        ca.cmd("DEL", "only_a")
        time.sleep(1.5)
        check("DEL ramane local (limita asumata)", cb.cmd("GET", "only_a") == ("bulk", b"1"))
        ca.close()
        cb.close()
    finally:
        a.cleanup()
        b.cleanup()


def sec_peer_restart(check):
    a, b = node(1, PORT_A, PORT_B), node(2, PORT_B, PORT_A)
    try:
        a.start()
        b.start()
        b.stop()  # peer oprit in timpul scrierilor
        ca = Client(a)
        keys = [f"p{i}" for i in range(200)]
        for key in keys:
            ca.cmd("SET", key, key)
        b.wait_port_free()
        b.start()
        cb = Client(b)
        check("peer-ul repornit converge", wait_until(lambda: cb.cmd("MGET", *keys)[1] == [("bulk", k.encode()) for k in keys]))
        cb.close()

        ca.close()
        a.restart()  # offsetul salvat: replicarea reia
        ca = Client(a)
        check("offsetul se salveaza", os.path.exists(os.path.join(a.workdir, f"replication_127.0.0.1_{PORT_B}.offset")))
        ca.cmd("SET", "after_restart", "1")
        cb = Client(b)
        check("replicarea reia dupa restart", wait_until(lambda: cb.cmd("GET", "after_restart") == ("bulk", b"1")))
        ca.close()
        cb.close()
    finally:
        a.cleanup()
        b.cleanup()


def sec_rewrite(check):
    a, b = node(1, PORT_A, PORT_B), node(2, PORT_B, PORT_A)
    try:
        a.start()
        b.start()
        ca, cb = Client(a), Client(b)
        ca.cmd("SET", "pre", "1")
        check("inainte de rescriere", wait_until(lambda: cb.cmd("GET", "pre") == ("bulk", b"1")))
        check("REWRITEAOF", ca.cmd("REWRITEAOF") == ("ok", "OK"))
        ca.cmd("SET", "post", "1")
        check("replicarea continua dupa rescriere", wait_until(lambda: cb.cmd("GET", "post") == ("bulk", b"1")))
        check("starea veche ramane", cb.cmd("GET", "pre") == ("bulk", b"1"))
        ca.close()
        cb.close()
    finally:
        a.cleanup()
        b.cleanup()


def main():
    fails = []
    for sec in (sec_converge, sec_peer_restart, sec_rewrite):
        run_with_retry(sec, fails)
    report(fails, "s10")


if __name__ == "__main__":
    main()
