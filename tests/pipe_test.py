"""PIPE: AOF-ul scris o data pe iteratie si prefetch-ul pentru pipeline.

Verifica: un batch confirmat (si cel cu chei in cold storage, si cel cu DEL-uri)
supravietuieste unui kill -9; sub always nimic nu ramane pending dupa ack; un
abonat nu primeste un PUBLISH inainte ca scrierile de dinaintea lui sa fie in
AOF; inregistrari mai mari decat bufferul de batch; QUIT, o eroare de protocol si
AUTH in mijlocul unui chunk; si un model de referinta pentru pipeline-uri lungi
cu ROOM, SET, DEL, MSET si chei reincarcate din cold storage intre prefetch si
executie.
"""

import os
import random
import signal
import time
import urllib.request

from common import Client, Server, report, resp_cmd, run_with_retry

PORT = 6402


def metrics():
    body = urllib.request.urlopen("http://127.0.0.1:9090/metrics", timeout=5).read().decode()
    out = {}
    for ln in body.splitlines():
        if ln and not ln.startswith("#"):
            key, value = ln.split()
            if value.lstrip("-").isdigit():
                out[key] = int(value)
    return out


def crash_restart(srv):
    """kill -9: procesul nu mai apuca nimic la iesire; doar ce era deja scris conteaza."""
    srv.proc.kill()
    srv.proc.wait()
    srv._log.close()
    srv.wait_port_free()
    srv.start()


def pipeline(c, cmds):
    """Toate comenzile intr-un singur sendall, apoi cate un raspuns pentru fiecare."""
    c.s.sendall(b"".join(resp_cmd(*cmd) for cmd in cmds))
    return [c.read_reply() for _ in cmds]


def read_aof(srv):
    with open(os.path.join(srv.workdir, "appendonly.aof"), "rb") as f:
        return f.read()


def sec_acked_batch_survives_kill(check):
    # SOMNIUM_MAX_KEYS mic: o parte din chei sunt in cold storage cand vin DEL-urile
    srv = Server(port=PORT, env_extra={"SOMNIUM_NO_METRICS": "1", "SOMNIUM_MAX_KEYS": "200"},
                 keep_dir_on_retry=True)
    try:
        srv.start()
        c = Client(srv)
        rng = random.Random(7)
        values = {f"k{i}": os.urandom(rng.randrange(1, 300)) for i in range(2000)}
        deleted = {f"k{i}" for i in range(0, 2000, 7)}
        cmds = [("SET", k, v) for k, v in values.items()]
        cmds += [("DEL", k) for k in sorted(deleted)]
        cmds += [("SET", "k1", b"rescris"), ("GET", "k3")]
        replies = pipeline(c, cmds)
        values["k1"] = b"rescris"
        check("2000 SET confirmate", all(r == ("ok", "OK") for r in replies[:2000]))
        check("fiecare DEL a gasit cheia (si in cold storage)",
              all(r == ("int", 1) for r in replies[2000:2000 + len(deleted)]))
        check("GET din acelasi batch vede SET-ul", replies[-1] == ("bulk", values["k3"]))
        c.close()

        crash_restart(srv)
        c = Client(srv)
        got = pipeline(c, [("GET", k) for k in values])
        lost = [k for k, r in zip(values, got) if k not in deleted and r != ("bulk", values[k])]
        revived = [k for k, r in zip(values, got) if k in deleted and r != ("nil", None)]
        check("dupa kill -9: fiecare scriere confirmata, cu valoarea exacta", not lost, lost[:5])
        check("dupa kill -9: cheile sterse raman sterse", not revived, revived[:5])
        c.close()
    finally:
        srv.cleanup()


def sec_always_batch(check):
    # fdatasync intarziat: un ack trimis inaintea scrierii batch-ului ar sosi imediat
    srv = Server(port=PORT, env_extra={"SOMNIUM_AOF_SYNC": "always", "SOMNIUM_AOF_SYNC_DELAY_MS": "250"},
                 keep_dir_on_retry=True)
    try:
        srv.start()
        c = Client(srv)
        t0 = time.time()
        c.s.sendall(b"".join(resp_cmd("SET", f"a{i}", str(i)) for i in range(500)))
        first = c.read_reply()
        lat = time.time() - t0
        replies = [first] + [c.read_reply() for _ in range(499)]
        check("always: 500 SET pipelined confirmate", all(r == ("ok", "OK") for r in replies))
        check("always: primul ack asteapta fdatasync-ul batch-ului", lat >= 0.20, f"{lat * 1000:.0f} ms")
        check("always: nimic pending dupa ack-ul batch-ului", metrics().get("db_aof_pending_bytes") == 0, metrics())
        c.close()
        crash_restart(srv)
        c = Client(srv)
        got = pipeline(c, [("GET", f"a{i}") for i in range(500)])
        check("always: batch-ul e pe disc dupa kill -9", all(r == ("bulk", str(i).encode()) for i, r in enumerate(got)))
        c.close()
    finally:
        srv.cleanup()


def sec_publish_after_write(check):
    # SET si PUBLISH, urmate de mii de GET (fara AOF): fara flush la PUBLISH, abonatul
    # ar primi mesajul cat SET-ul asteapta inca write-ul de la capatul iteratiei
    srv = Server(port=PORT, env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
    try:
        srv.start()
        sub = Client(srv)
        check("abonat", sub.cmd("SUBSCRIBE", "ch")[0] == "array")
        pub = Client(srv)
        cmds = [("SET", "pk", "pv"), ("PUBLISH", "ch", "gata")] + [("GET", "nimic")] * 8000
        pub.s.sendall(b"".join(resp_cmd(*cmd) for cmd in cmds))

        msg = sub.read_reply()
        os.kill(srv.proc.pid, signal.SIGSTOP) # serverul ingheata, de obicei in mijlocul batch-ului
        try:
            aof = read_aof(srv)
        finally:
            os.kill(srv.proc.pid, signal.SIGCONT)
        check("abonatul primeste mesajul", msg == ("array", [("bulk", b"message"), ("bulk", b"ch"), ("bulk", b"gata")]), msg)
        check("cand mesajul ajunge, SET-ul de dinainte e deja in AOF", b"$2\r\npk\r\n$2\r\npv\r\n" in aof)

        replies = [pub.read_reply() for _ in cmds]
        check("publisher: SET, PUBLISH (1 abonat), GET-uri", replies[:2] == [("ok", "OK"), ("int", 1)], replies[:2])
        sub.close()
        pub.close()
        crash_restart(srv)
        c = Client(srv)
        check("dupa kill -9: SET-ul anuntat exista", c.cmd("GET", "pk") == ("bulk", b"pv"))
        c.close()
    finally:
        srv.cleanup()


def sec_records_larger_than_batch_buffer(check):
    # EXPIRE scrie toata valoarea in AOF: cateva comenzi mici depasesc bufferul de batch (1 MB)
    srv = Server(port=PORT, env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
    try:
        srv.start()
        c = Client(srv)
        big = os.urandom(700 * 1024)
        check("SET mare", c.cmd("SET", "big", big) == ("ok", "OK"))
        replies = pipeline(c, [("EXPIRE", "big", "1000")] * 6 + [("SET", "dupa", "1")])
        check("6 EXPIRE + SET confirmate", replies == [("int", 1)] * 6 + [("ok", "OK")], replies)
        c.close()
        crash_restart(srv)
        c = Client(srv)
        check("valoarea mare intreaga dupa kill -9", c.cmd("GET", "big") == ("bulk", big))
        check("TTL-ul pastrat", 0 < c.cmd("TTL", "big")[1] <= 1000)
        check("scrierea de dupa ele", c.cmd("GET", "dupa") == ("bulk", b"1"))
        c.close()
    finally:
        srv.cleanup()


def sec_chunk_edges(check):
    srv = Server(port=PORT, env_extra={"SOMNIUM_NO_METRICS": "1", "SOMNIUM_PASSWORD": "parola"},
                 keep_dir_on_retry=True)
    try:
        srv.start()
        # AUTH in mijlocul chunk-ului: comenzile de dinainte raman NOAUTH
        c = Client(srv)
        replies = pipeline(c, [("SET", "x", "1"), ("AUTH", "parola"), ("SET", "y", "2"), ("GET", "y"), ("GET", "x")])
        check("AUTH in chunk: NOAUTH, OK, OK, y, x absent",
              [r[0] for r in replies[:3]] == ["err", "ok", "ok"] and replies[3:] == [("bulk", b"2"), ("nil", None)],
              replies)

        # QUIT in mijlocul chunk-ului: ce urmeaza nu se executa
        c.s.sendall(resp_cmd("SET", "qa", "1") + resp_cmd("QUIT") + resp_cmd("SET", "qb", "2"))
        replies = [c.read_reply() for _ in range(2)]
        check("QUIT in chunk: OK, OK", replies == [("ok", "OK"), ("ok", "OK")], replies)
        c.close()

        # eroare de protocol dupa comenzi valide din acelasi chunk
        c = Client(srv)
        c.s.sendall(resp_cmd("AUTH", "parola") + resp_cmd("SET", "pa", "1") + resp_cmd("SET", "pb", "2") + b"*1\r\n$x\r\n")
        replies = [c.read_reply() for _ in range(4)]
        check("eroare de protocol in chunk: OK, OK, OK, -ERR",
              replies[:3] == [("ok", "OK")] * 3 and replies[3][0] == "err", replies)
        c.close()

        crash_restart(srv)
        c = Client(srv)
        c.cmd("AUTH", "parola")
        got = pipeline(c, [("GET", k) for k in ("y", "qa", "qb", "pa", "pb")])
        check("dupa kill -9: y, qa, pa, pb confirmate; qb nu s-a executat",
              got == [("bulk", b"2"), ("bulk", b"1"), ("nil", None), ("bulk", b"1"), ("bulk", b"2")], got)
        c.close()
    finally:
        srv.cleanup()


def sec_pipeline_model(check):
    # chunk-urile sunt prefetch-uite inainte de executie, apoi executia insereaza,
    # sterge, rehash-uieste si reincarca din cold storage: raspunsurile trebuie sa fie
    # exact ale unui model executat comanda cu comanda
    srv = Server(port=PORT, env_extra={"SOMNIUM_NO_METRICS": "1", "SOMNIUM_MAX_KEYS": "150"},
                 keep_dir_on_retry=True)
    try:
        srv.start()
        rng = random.Random(11)
        rooms = ["default", "r1", "r2"]
        model = {r: {} for r in rooms}
        room = "default"
        cmds, expected = [], []

        def key():
            return f"m{rng.randrange(400)}"

        for _ in range(20000):
            op = rng.random()
            data = model[room]
            if op < 0.02:
                room = rng.choice(rooms)
                cmds.append(("ROOM", room))
                expected.append(("ok", "OK"))
            elif op < 0.40:
                k, v = key(), os.urandom(rng.randrange(0, 120))
                data[k] = v
                cmds.append(("SET", k, v))
                expected.append(("ok", "OK"))
            elif op < 0.70:
                k = key()
                cmds.append(("GET", k))
                expected.append(("bulk", data[k]) if k in data else ("nil", None))
            elif op < 0.80:
                ks = [key() for _ in range(rng.randrange(1, 4))]
                n = 0
                for k in ks:
                    if data.pop(k, None) is not None:
                        n += 1
                cmds.append(("DEL", *ks))
                expected.append(("int", n))
            elif op < 0.87:
                ks = [key() for _ in range(rng.randrange(1, 4))]
                cmds.append(("EXISTS", *ks))
                expected.append(("int", sum(k in data for k in ks)))
            elif op < 0.94:
                pairs = [(key(), os.urandom(rng.randrange(0, 60))) for _ in range(rng.randrange(1, 4))]
                for k, v in pairs:
                    data[k] = v
                cmds.append(("MSET", *[x for p in pairs for x in p]))
                expected.append(("ok", "OK"))
            else:
                ks = [key() for _ in range(rng.randrange(1, 5))]
                cmds.append(("MGET", *ks))
                expected.append(("array", [("bulk", data[k]) if k in data else ("nil", None) for k in ks]))

        c = Client(srv)
        got = pipeline(c, cmds)
        bad = [i for i, (g, e) in enumerate(zip(got, expected)) if g != e]
        check("20000 comenzi pipelined = modelul", not bad,
              [(i, cmds[i][:2], got[i], expected[i]) for i in bad[:3]])
        c.close()

        crash_restart(srv)
        c = Client(srv)
        wrong = []
        for r in rooms:
            c.cmd("ROOM", r)
            ks = [f"m{i}" for i in range(400)]
            for k, g in zip(ks, pipeline(c, [("GET", k) for k in ks])):
                if g != (("bulk", model[r][k]) if k in model[r] else ("nil", None)):
                    wrong.append((r, k))
        check("dupa kill -9: fiecare camera = modelul", not wrong, wrong[:5])
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_acked_batch_survives_kill, sec_always_batch, sec_publish_after_write,
                sec_records_larger_than_batch_buffer, sec_chunk_edges, sec_pipeline_model):
        run_with_retry(sec, fails)
    report(fails, "pipe")


if __name__ == "__main__":
    main()
