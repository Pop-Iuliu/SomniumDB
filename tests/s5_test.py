"""S5 "calcaneoscaphoid": contracte de persistenta.

Verifica: formatul AOF v3 (header + meta mutatie), redarea exacta a cozii
complete la trunchiere pe fiecare byte al ultimei comenzi, migrarea explicita
v1/v2 -> v3, camera numita literal "SET", ordinea CRDT pastrata la restart,
injctii de esec la snapshot (scriere/rename/coruptie) si politica de
durabilitate.
"""

import os
import socket
import struct
import time

from common import Server, resp_cmd, recv_until, report


class Client:
    """Client RESP minimal, cu drain complet per comanda."""

    def __init__(self, srv):
        self.s = srv.connect()

    def cmd(self, *args):
        self.s.sendall(resp_cmd(*args))
        return self.read_reply()

    def read_reply(self):
        s = self.s
        s.settimeout(10)
        first = b""
        while not first.endswith(b"\r\n"):
            chunk = s.recv(4096)
            if not chunk:
                raise ConnectionError("serverul a inchis conexiunea")
            first += chunk
        t, rest = first[0:1], first[1:]
        if t == b"+":
            return ("ok", rest[:-2].decode())
        if t == b"-":
            return ("err", rest[:-2].decode())
        if t == b":":
            return ("int", int(rest[:-2]))
        if t == b"$":
            length = int(rest.split(b"\r\n", 1)[0])
            if length == -1:
                return ("nil", None)
            want = len(b"$" + str(length).encode() + b"\r\n") + length + 2
            while len(first) < want:
                chunk = s.recv(want - len(first))
                if not chunk:
                    raise ConnectionError("răspuns trunchiat")
                first += chunk
            payload = first[len(b"$" + str(length).encode() + b"\r\n"):want - 2]
            return ("bulk", payload)
        raise AssertionError(f"răspuns necunoscut: {first!r}")

    def close(self):
        self.s.close()


def bulk(b: bytes) -> bytes:
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"


def v3_record(room: bytes, args: list, expire=b"0", ts=b"0", node=b"1") -> bytes:
    toks = [room] + [a if isinstance(a, bytes) else a.encode() for a in args]
    toks += [b"SOMNIUM-META", expire, ts, node]
    return b"*" + str(len(toks)).encode() + b"\r\n" + b"".join(bulk(t) for t in toks)


AOF_HDR = b"*2\r\n$11\r\nSOMNIUM-AOF\r\n$1\r\n3\r\n"


def read_aof(workdir):
    p = os.path.join(workdir, "appendonly.aof")
    if not os.path.exists(p):
        return None
    with open(p, "rb") as f:
        return f.read()


def find_file(workdir, prefix):
    return [f for f in os.listdir(workdir) if f.startswith(prefix)]


def write_snapshot_v2(path, records):
    """records: lista de (key, value, expire, ts, node). Format RSNP v2."""
    with open(path, "wb") as f:
        f.write(b"RSNP")
        f.write(struct.pack("<I", 2))
        f.write(struct.pack("<Q", len(records)))
        for key, val, expire, ts, node in records:
            f.write(struct.pack("<Q", len(key)))
            f.write(key)
            f.write(struct.pack("<Q", len(val)))
            f.write(val)
            f.write(struct.pack("<q", expire))
            f.write(struct.pack("<Q", ts))
            f.write(struct.pack("<I", node))


def run_with_retry(fn, fails, max_attempts=3):
    """Ruleaza o sectiune de test, reiaut de la zero la deces de server.

    Flake-ul io_uring documentat in AGENTS.md poate livra un server fara
    nicio completare (conexiunile atarna sau se resateaza); sectiunile sunt
    self-contained (Server curat de fiecare data), deci le putem re-rula.
    Esecurile reale de asertie raman consemnate la ultima incercare.
    """
    for attempt in range(max_attempts):
        local_fails = []

        def check(name, cond, extra="", _lf=local_fails):
            print(("PASS" if cond else "FAIL"), name, extra if not cond else "")
            if not cond:
                _lf.append(name)

        try:
            fn(check)
            fails.extend(local_fails)
            return
        except (ConnectionError, TimeoutError, OSError, RuntimeError, AssertionError) as e:
            if attempt == max_attempts - 1:
                print(f"  sectiune esuata definitiv: {type(e).__name__} {e}")
                fails.extend(local_fails)
                fails.append(f"sectiune prabusita: {type(e).__name__}: {e}")
            else:
                print(f"  (sectiune reiauta dupa {type(e).__name__})")


def main():
    fails = []

    # ------------------------------------------------------------------
    # 1. Format v3: header in fisier, camera numita "SET", CRDT la restart
    # ------------------------------------------------------------------
    def sec1(check):
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"})
        try:
            srv.start()
            c = Client(srv)
            c.cmd("SET", "foo", "bar")
            c.cmd("ROOM", "SET")
            c.cmd("SET", "k", "v")
            c.cmd("ROOM", "alpha")
            c.cmd("SET", "a", "1")
            c.cmd("CRDTMERGE", "ck", "cv", "424242", "42")
            c.close()

            aof = read_aof(srv.workdir)
            check("AOF incepe cu headerul v3", aof is not None and aof.startswith(AOF_HDR), aof[:40] if aof else None)

            srv.restart()
            c = Client(srv)
            check("restart: foo", c.cmd("GET", "foo") == ("bulk", b"bar"))
            check("restart: camera numita SET", c.cmd("ROOM", "SET")[0] == "ok")
            check("restart: k in camera SET", c.cmd("GET", "k") == ("bulk", b"v"))
            c.cmd("ROOM", "alpha")
            check("restart: ck convergent", c.cmd("GET", "ck") == ("bulk", b"cv"))
            r = c.cmd("CRDTMERGE", "ck", "STALE", "5", "1")
            check("restart: ts CRDT verbatim (stale ignorat)", "Ignored" in r[1], r)
            c.close()
        finally:
            srv.cleanup()

    run_with_retry(sec1, fails)

    # ------------------------------------------------------------------
    # 2 + 3. Trunchierea ultimei comenzi pe fiecare byte; apend supravietuieste
    # ------------------------------------------------------------------
    room = b"rt"
    r1 = v3_record(room, [b"SET", b"k1", b"v1"])
    r2 = v3_record(room, [b"SET", b"k2", b"v2"])
    r3 = v3_record(room, [b"SET", b"k3", b"v"])

    def sec2(check):
        n_bad = 0
        # flake-ul de mediu io_uring (documentat in AGENTS.md) loveste mai des
        # la churn rapid de servere: fiecare taiere primeste pana la 3 incercari
        for cut in range(len(r3)):
            cut_ok = False
            for attempt in range(3):
                srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
                try:
                    with open(os.path.join(srv.workdir, "appendonly.aof"), "wb") as f:
                        f.write(AOF_HDR + r1 + r2 + r3[:cut])
                    srv.start()
                    c = Client(srv)
                    c.cmd("ROOM", "rt")
                    cut_ok = (c.cmd("GET", "k1") == ("bulk", b"v1")
                              and c.cmd("GET", "k2") == ("bulk", b"v2")
                              and c.cmd("GET", "k3")[0] == "nil")
                    c.close()
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"  trunchiere la byte {cut}: server esuat dupa 3 incercari: {type(e).__name__} {e}")
                        log = os.path.join(srv.workdir, "server.2.log")
                        if os.path.exists(log):
                            print("   --- log server ---")
                            print(open(log, "rb").read().decode(errors="replace")[-500:])
                finally:
                    srv.cleanup()
            if not cut_ok:
                n_bad += 1
                print(f"  trunchiere la byte {cut}: istoricul complet NU a fost respectat")
        check(f"trunchiere finala pe fiecare din {len(r3)} bytes -> doar comenzi complete", n_bad == 0,
              f"{n_bad} esuri")

    def sec3(check):
        for cut in (0, len(r3) // 3, len(r3) - 1):
            for attempt in range(3):
                srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
                try:
                    with open(os.path.join(srv.workdir, "appendonly.aof"), "wb") as f:
                        f.write(AOF_HDR + r1 + r2 + r3[:cut])
                    srv.start()
                    c = Client(srv)
                    c.cmd("ROOM", "rt")
                    check(f"append dupa trunchiere (cut={cut}): k1/k2", c.cmd("GET", "k1") == ("bulk", b"v1"))
                    c.cmd("SET", "fresh", "yes")
                    c.close()
                    srv.restart()
                    c = Client(srv)
                    c.cmd("ROOM", "rt")
                    check(f"append dupa trunchiere (cut={cut}): fresh", c.cmd("GET", "fresh") == ("bulk", b"yes"))
                    c.close()
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                finally:
                    srv.cleanup()

    run_with_retry(sec2, fails)
    run_with_retry(sec3, fails)

    # ------------------------------------------------------------------
    # 4. Migrarea legacy v1 (comenzi fara camera) catre v3
    # ------------------------------------------------------------------
    def sec4(check):
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
        try:
            legacy = (b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
                      b"*2\r\n$3\r\nDEL\r\n$1\r\na\r\n"
                      b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n")
            with open(os.path.join(srv.workdir, "appendonly.aof"), "wb") as f:
                f.write(legacy)
            srv.start()
            c = Client(srv)
            check("v1: a sters", c.cmd("GET", "a")[0] == "nil")
            check("v1: b redat", c.cmd("GET", "b") == ("bulk", b"2"))
            aof = read_aof(srv.workdir)
            check("v1 migrat: headerul v3 prezent", aof is not None and aof.startswith(AOF_HDR), aof[:40] if aof else None)
            check("v1 migrat: fara .tmp ramas", not os.path.exists(os.path.join(srv.workdir, "appendonly.aof.tmp")))
            c.close()
            srv.restart()
            c = Client(srv)
            check("v1 migrat: round-trip restart", c.cmd("GET", "b") == ("bulk", b"2"))
            c.close()
        finally:
            srv.cleanup()

    # ------------------------------------------------------------------
    # 5. Migrarea legacy v2 (camera + comanda, fara meta) catre v3
    # ------------------------------------------------------------------
    def sec5(check):
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
        try:
            # v2: [room, CMD, args...] fara meta si fara header
            v2 = (b"*4\r\n" + bulk(b"beta") + bulk(b"SET") + bulk(b"bk") + bulk(b"bv")
                  + b"*4\r\n" + bulk(b"gamma") + bulk(b"SET") + bulk(b"gk") + bulk(b"gv"))
            with open(os.path.join(srv.workdir, "appendonly.aof"), "wb") as f:
                f.write(v2)
            srv.start()
            c = Client(srv)
            c.cmd("ROOM", "beta")
            check("v2: bk in beta", c.cmd("GET", "bk") == ("bulk", b"bv"))
            c.cmd("ROOM", "gamma")
            check("v2: gk in gamma", c.cmd("GET", "gk") == ("bulk", b"gv"))
            aof = read_aof(srv.workdir)
            check("v2 migrat: headerul v3 prezent", aof is not None and aof.startswith(AOF_HDR))
            c.close()
            srv.restart()
            c = Client(srv)
            c.cmd("ROOM", "beta")
            check("v2 migrat: round-trip restart", c.cmd("GET", "bk") == ("bulk", b"bv"))
            c.close()
        finally:
            srv.cleanup()

    # ------------------------------------------------------------------
    # 6. Coruptie in mijlocul AOF-ului: pastrata pentru diagnostic, AOF nou pornit
    # ------------------------------------------------------------------
    def sec6(check):
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
        try:
            corrupt = AOF_HDR + b"XXXX\r\n" + r1 + r2
            with open(os.path.join(srv.workdir, "appendonly.aof"), "wb") as f:
                f.write(corrupt)
            srv.start()
            c = Client(srv)
            c.cmd("ROOM", "rt")
            # coruperea e la inceput: nimic nu se redate, dar serverul functioneaza
            check("corupt mijloc: k1 nil (inainte de coruptie nimic)", c.cmd("GET", "k1")[0] == "nil")
            c.cmd("SET", "post", "p1")
            c.close()
            check("corupt mijloc: fisier pastrat pt diagnostic",
                  len(find_file(srv.workdir, "appendonly.aof.corrupt")) == 1,
                  find_file(srv.workdir, "appendonly.aof.corrupt"))
            srv.restart()
            c = Client(srv)
            c.cmd("ROOM", "rt")
            check("corupt mijloc: appendul de dupa a supravietuit", c.cmd("GET", "post") == ("bulk", b"p1"))
            c.close()
        finally:
            srv.cleanup()

    # ------------------------------------------------------------------
    # 7. Ordinea CRDT nu se schimba la replay (timestampuri NU regenerate)
    # ------------------------------------------------------------------
    def sec7(check):
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"})
        try:
            srv.start()
            c = Client(srv)
            now = int(time.time() * 1000)
            c.cmd("SET", "ord", "local")
            # un merge cu ts usor peste SET-ul local: converge acum si TREBUIE
            # sa converge si dupa restart (replay-ul vechi regenera timestampuri
            # noi si il ignora)
            r = c.cmd("CRDTMERGE", "ord", "remote", str(now + 1000), "9")
            check("merge peste SET", "Converged" in r[1], r)
            c.close()
            srv.restart()
            c = Client(srv)
            r = c.cmd("CRDTMERGE", "ord", "remote2", str(now + 2000), "9")
            check("merge mai nou dupa restart: convergent", "Converged" in r[1], r)
            check("valoarea dupa merge", c.cmd("GET", "ord") == ("bulk", b"remote2"))
            r = c.cmd("CRDTMERGE", "ord", "stale", str(now + 1500), "9")
            check("merge stale dupa restart: ignorat", "Ignored" in r[1], r)
            c.close()
        finally:
            srv.cleanup()

    run_with_retry(sec4, fails)
    run_with_retry(sec5, fails)
    run_with_retry(sec6, fails)
    run_with_retry(sec7, fails)

    # ------------------------------------------------------------------
    # 8. Snapshot corupt: magic prost / versiune necunoscuta / trunchiat la mijloc
    # ------------------------------------------------------------------
    def sec8(check):
        for name, blob in (
            ("magic prost", b"GARBAGE-GARBAGE-9"),
            ("versiune necunoscuta", b"RSNP" + struct.pack("<I", 99) + struct.pack("<Q", 1)),
            ("trunchiat la mijloc", None),  # construit mai jos
        ):
            srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
            try:
                snap = os.path.join(srv.workdir, "room_cs.bin")
                if blob is None:
                    # header valid + o inregistrare completa + 3 bytes in plus
                    with open(snap, "wb") as f:
                        f.write(b"RSNP" + struct.pack("<I", 2) + struct.pack("<Q", 2))
                        rec = struct.pack("<Q", 2) + b"k1" + struct.pack("<Q", 2) + b"v1" \
                              + struct.pack("<q", 0) + struct.pack("<Q", 7) + struct.pack("<I", 1)
                        f.write(rec + b"XYZ")
                else:
                    with open(snap, "wb") as f:
                        f.write(blob)
                srv.start()
                c = Client(srv)
                c.cmd("ROOM", "cs")
                check(f"snapshot {name}: camera folosibila (goala)", c.cmd("GET", "k1")[0] == "nil")
                c.cmd("SET", "nk", "nv")
                c.close()
                check(f"snapshot {name}: pastrat pt diagnostic, nu sters",
                      len(find_file(srv.workdir, "room_cs.bin.corrupt")) == 1,
                      os.listdir(srv.workdir))
                srv.restart()
                c = Client(srv)
                c.cmd("ROOM", "cs")
                check(f"snapshot {name}: serverul functioneaza normal dupa", c.cmd("GET", "nk") == ("bulk", b"nv"))
                c.close()
            finally:
                srv.cleanup()

    # ------------------------------------------------------------------
    # 9. Hibernare cu esec de scriere (tmp e director): camera ramane activa
    # ------------------------------------------------------------------
    def sec9(check):
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"})
        try:
            srv.start()
            c = Client(srv)
            c.cmd("ROOM", "wf")
            c.cmd("SET", "wk", "wv")
            c.close()
            os.mkdir(os.path.join(srv.workdir, "room_wf.bin.tmp"))  # injectie: open() esueaza
            print("astept 16s pentru watchdog (hibernare cu esec injectat)...")
            time.sleep(16)
            c = Client(srv)
            c.cmd("ROOM", "wf")
            r = c.cmd("GET", "wk")
            check("esec scriere snapshot: inregistrarile raman citibile", r == ("bulk", b"wv"), r)
            c.close()
        finally:
            srv.cleanup()

    # ------------------------------------------------------------------
    # 10. Hibernare cu esec de rename (target e director): inregistrarile raman
    # ------------------------------------------------------------------
    def sec10(check):
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"})
        try:
            srv.start()
            c = Client(srv)
            c.cmd("ROOM", "rf")
            c.cmd("SET", "rk", "rv")
            c.close()
            os.mkdir(os.path.join(srv.workdir, "room_rf.bin"))  # injectie: rename() esueaza
            print("astept 16s pentru watchdog (rename cu esec injectat)...")
            time.sleep(16)
            c = Client(srv)
            c.cmd("ROOM", "rf")
            r = c.cmd("GET", "rk")
            check("esec rename snapshot: inregistrarile raman citibile", r == ("bulk", b"rv"), r)
            c.close()
        finally:
            srv.cleanup()

    # ------------------------------------------------------------------
    # 11. Politica de durabilitate (SOMNIUM_AOF_SYNC)
    # ------------------------------------------------------------------
    def sec11(check):
        for env_val, expected in (("always", "always"), ("everysec", "everysec"), ("no", "no")):
            srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1", "SOMNIUM_AOF_SYNC": env_val})
            try:
                srv.start()
                c = Client(srv)
                r = c.cmd("SET", "dp", "1")
                check(f"politica {env_val}: SET functioneaza", r[0] == "ok", r)
                log = ""
                for f in sorted(os.listdir(srv.workdir)):
                    if f.startswith("server") and f.endswith(".log"):
                        with open(os.path.join(srv.workdir, f), "rb") as fh:
                            log = fh.read().decode(errors="replace")
                check(f"politica {env_val}: afisata la pornire", f"politica durabilitate: {expected}" in log,
                      log[-200:] if log else "fara log")
                c.close()
            finally:
                srv.cleanup()

        # default = everysec
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"})
        try:
            srv.start()
            c = Client(srv)
            c.cmd("SET", "dp", "1")
            c.close()
            log = ""
            for f in sorted(os.listdir(srv.workdir)):
                if f.startswith("server") and f.endswith(".log"):
                    with open(os.path.join(srv.workdir, f), "rb") as fh:
                        log = fh.read().decode(errors="replace")
            check("politica default: everysec", "politica durabilitate: everysec" in log)
        finally:
            srv.cleanup()

    # ------------------------------------------------------------------
    # 12. Recovery >3 camere: mutatiile nu se pierd (recovery trece peste buget)
    # ------------------------------------------------------------------
    def sec12(check):
        srv = Server(env_extra={"SOMNIUM_NO_METRICS": "1"}, keep_dir_on_retry=True)
        try:
            fixture = AOF_HDR + b"".join(
                v3_record(f"room{i}".encode(), [b"SET", b"pk", b"pv"]) for i in range(5)
            )
            with open(os.path.join(srv.workdir, "appendonly.aof"), "wb") as f:
                f.write(fixture)
            srv.start()
            c = Client(srv)
            ok = True
            for i in range(5):
                c.cmd("ROOM", f"room{i}")
                if c.cmd("GET", "pk") != ("bulk", b"pv"):
                    ok = False
            check("recovery peste bugetul de 3 camere", ok)
            c.close()
        finally:
            srv.cleanup()

    for sec in (sec8, sec9, sec10, sec11, sec12):
        run_with_retry(sec, fails)

    report(fails, "s5")


if __name__ == "__main__":
    main()
