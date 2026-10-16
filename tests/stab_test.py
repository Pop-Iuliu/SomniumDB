"""Stabilitate: regresii gasite la testul de stres.

Verifica: o scriere partiala (disc plin) nu lasa o inregistrare rupta nici in AOF
(altfel replay-ul pierde tot ce urmeaza dupa ea), nici in cold storage (altfel
indexul citeste valoarea altei chei). Discul plin e simulat fara root: limita de
marime a fisierelor procesului (RLIMIT_FSIZE, prin prlimit) face ca urmatoarea
scriere mare sa ajunga partial si sa esueze cu EFBIG, iar ridicarea ei e spatiul
eliberat. Limita de fd-uri: SOMNIUM_MAXCLIENTS incape in ea (ridicata sau
micsorata la pornire), iar fara fd-uri acceptul nu intra in bucla.
"""

import os
import resource
import socket
import stat
import tempfile
import time

from common import REPO, Client, Server, report, resp_cmd, run_with_retry, wait_until

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


def sec_saturated_eviction(check):
    # dupa ~40k accesari count-min sketch-ul e saturat: nicio cheie nu mai pare rece, iar
    # cautarea unei victime reci (acum marginita) trebuie sa evicteze corect in continuare
    srv = Server(env_extra={**ENV, "SOMNIUM_MAX_KEYS": "500"})
    try:
        srv.start()
        c = Client(srv)
        mset = ["MSET"]
        for i in range(3000):
            mset += [f"k{i}", f"v{i}"]
        c.cmd(*mset)
        c.s.sendall(b"".join(resp_cmd("GET", f"k{i % 3000}") for i in range(45000)))
        for _ in range(45000):
            c.read_reply()
        for i in range(3000, 4000):
            c.cmd("SET", f"k{i}", f"v{i}")
        keys = [ln for ln in c.cmd("INFO")[1].decode().splitlines() if ln.startswith("Chei totale")]
        check("sketch saturat: camera tine exact SOMNIUM_MAX_KEYS in RAM", keys == ["Chei totale: 500"], keys)
        wrong = [i for i in range(4000) if c.cmd("GET", f"k{i}") != ("bulk", f"v{i}".encode())]
        check("sketch saturat: fiecare cheie (RAM sau cold) isi citeste valoarea", not wrong, wrong[:10])
        c.close()
    finally:
        srv.cleanup()


def sec_replication_misaligned_offset(check):
    # offsetul salvat cu acelasi inode dar in mijlocul unei inregistrari: ce lasa doua
    # rescrieri AOF pe ext4 (inode-ul eliberat e refolosit) cat timp peer-ul e oprit
    peer = lambda node, port, other: Server(port=port, keep_dir_on_retry=True, env_extra={
        **ENV, "SOMNIUM_NODE_ID": str(node), "SOMNIUM_PEERS": f"127.0.0.1:{other}"})
    a, b = peer(1, 6391, 6392), peer(2, 6392, 6391)
    try:
        a.start()
        b.start()
        ca, cb = Client(a), Client(b)
        ca.cmd("SET", "k1", "v1")
        check("pregatire: k1 se replica", wait_until(lambda: cb.cmd("GET", "k1") == ("bulk", b"v1")))
        ca.close()
        a.stop()
        inode = os.stat(os.path.join(a.workdir, "appendonly.aof")).st_ino
        with open(os.path.join(a.workdir, "replication_127.0.0.1_6392.offset"), "w") as f:
            f.write(f"{inode} 7\n")  # in mijlocul headerului
        a.wait_port_free()
        a.start()
        ca = Client(a)
        ca.cmd("SET", "k2", "v2")
        check("offset nealiniat: replicarea isi revine",
              wait_until(lambda: cb.cmd("GET", "k2") == ("bulk", b"v2"), timeout=10))
        ca.close()
        cb.close()
    finally:
        a.cleanup()
        b.cleanup()


def with_ulimit(flags):
    """Server pornit printr-un wrapper care ii seteaza limita de fd-uri inainte de exec."""
    real = os.path.abspath(os.environ.get("SOMNIUM_BIN") or os.path.join(REPO, "build-release", "Redis"))
    fd, wrapper = tempfile.mkstemp(prefix="somnium-ulimit-", suffix=".sh")
    with os.fdopen(fd, "w") as f:
        f.write(f'#!/bin/sh\nulimit {flags}\nexec "{real}"\n')
    os.chmod(wrapper, stat.S_IRWXU)
    return Server(bin_path=wrapper, env_extra=ENV), wrapper


def connect_many(srv, n):
    socks = []
    for _ in range(n):
        s = socket.create_connection(("127.0.0.1", srv.port), timeout=5)
        socks.append(s)
    return socks


def answers(s):
    """Eroarea de cap atins (sosita deja, fara intrebare), PONG, sau None."""
    try:
        s.settimeout(0)  # cu timeout, Python asteapta intai tot timeout-ul, chiar si cu MSG_DONTWAIT
        try:
            early = s.recv(100)
            if early:
                return early
        except BlockingIOError:
            pass
        s.settimeout(2)
        s.sendall(b"PING\r\n")
        return s.recv(100)
    except OSError:
        return None


def sec_fd_limit(check):
    # doar limita moale e mica: serverul o ridica pentru cei 10000 de clienti impliciti
    srv, wrapper = with_ulimit("-S -n 64")
    try:
        srv.start()
        socks = connect_many(srv, 100)
        ok = sum(answers(s) == b"+PONG\r\n" for s in socks)
        check("limita moale ridicata: 100 de clienti peste ulimit -n 64", ok == 100, f"{ok}/100")
        for s in socks:
            s.close()
    finally:
        srv.cleanup()
        os.unlink(wrapper)

    # si limita tare: SOMNIUM_MAXCLIENTS se micsoreaza, clientii in plus primesc eroarea
    srv, wrapper = with_ulimit("-n 64")
    try:
        srv.start()
        log = open(os.path.join(srv.workdir, "server.0.log"), "rb").read()
        check("limita tare: SOMNIUM_MAXCLIENTS micsorat si raportat", b"folosesc 32" in log, log[-300:])
        socks = connect_many(srv, 50)
        time.sleep(1)  # refuzurile pleaca la accept
        replies = [answers(s) for s in socks]
        pong = sum(r == b"+PONG\r\n" for r in replies)
        full = sum(r is not None and b"max number of clients" in r for r in replies)
        check("limita tare: 32 de clienti serviti, restul refuzati cu eroare", (pong, full) == (32, 18),
              (pong, full))
        for s in socks:
            s.close()
    finally:
        srv.cleanup()
        os.unlink(wrapper)


def sec_accept_backoff(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        # doar limita moale: cea tare nu mai poate fi ridicata fara root
        soft, hard = resource.prlimit(srv.proc.pid, resource.RLIMIT_NOFILE)
        resource.prlimit(srv.proc.pid, resource.RLIMIT_NOFILE, (40, hard))  # fd-urile se termina acum
        socks = connect_many(srv, 60)  # cele peste limita raman in backlog (accept: EMFILE)
        time.sleep(2)
        check("fara fd-uri: clientii existenti sunt serviti", c.cmd("PING") == ("ok", "PONG"))
        log = open(os.path.join(srv.workdir, "server.0.log"), "rb").read()
        lines = log.count(b"accept")
        check("fara fd-uri: acceptul nu intra in bucla (log agregat)", lines <= 5, f"{lines} linii")
        for s in socks:
            s.close()
        resource.prlimit(srv.proc.pid, resource.RLIMIT_NOFILE, (soft, hard))
        time.sleep(1.5)  # cel putin un sweep re-armeaza acceptul
        s = srv.connect()
        check("fd-uri eliberate: conexiunile noi sunt acceptate", answers(s) == b"+PONG\r\n")
        s.close()
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_torn_aof, sec_torn_cold, sec_saturated_eviction, sec_replication_misaligned_offset,
                sec_fd_limit, sec_accept_backoff):
        run_with_retry(sec, fails)
    report(fails, "stab")


if __name__ == "__main__":
    main()
