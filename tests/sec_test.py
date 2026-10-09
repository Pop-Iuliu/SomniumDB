"""Securitate (securityprogress.md): fiecare pas SEC-n isi aduce aici verificarea."""

import glob
import os
import shutil
import socket
import subprocess
import time
import urllib.request

from common import Client, Server, report, resp_cmd, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}
PW = "s3cret-pw"


def server_log(srv):
    with open(sorted(glob.glob(os.path.join(srv.workdir, "server.*.log")))[-1], "rb") as f:
        return f.read().decode(errors="replace")


def run_once(srv, env, timeout=5):
    """Porneste binarul cu env dat si asteapta sa iasa singur (erori de configurare)."""
    env = dict(os.environ, REDIS_PORT=str(srv.port), **ENV, **env)
    try:
        return subprocess.run([srv.bin], cwd=srv.workdir, env=env, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def sec1_bind(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        check("SEC-1: implicit asculta pe 127.0.0.1", f"127.0.0.1:{srv.port}" in server_log(srv), server_log(srv))
        srv.stop()

        r = run_once(srv, {"SOMNIUM_BIND": "not-an-ip"})
        check("SEC-1: SOMNIUM_BIND invalid iese cu codul 1", r is not None and r.returncode == 1, r)
    finally:
        srv.cleanup()


def wait_until(cond, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.2)
    return cond()


def sec2_auth(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_PASSWORD": PW})
    try:
        srv.start()
        c = Client(srv)
        check("SEC-2: GET inainte de AUTH", c.cmd("GET", "k") == ("err", "NOAUTH Authentication required."))
        check("SEC-2: HELLO fara AUTH", c.cmd("HELLO", "3")[1].startswith("NOAUTH"))
        check("SEC-2: parola gresita", c.cmd("AUTH", "nope") == ("err", "WRONGPASS invalid username-password pair"))
        check("SEC-2: alt utilizator", c.cmd("AUTH", "admin", PW)[0] == "err")
        check("SEC-2: parola corecta", c.cmd("AUTH", PW) == ("ok", "OK"))
        check("SEC-2: dupa AUTH comenzile merg", c.cmd("SET", "k", "v") == ("ok", "OK") and c.cmd("GET", "k") == ("bulk", b"v"))
        c.close()

        c = Client(srv)
        check("SEC-2: HELLO 3 AUTH intr-un pas", c.cmd("HELLO", "3", "AUTH", "default", PW)[0] == "map")
        check("SEC-2: ...apoi RESP3", c.cmd("GET", "nope") == ("null", None))
        check("SEC-2: AUTH default <pw>", c.cmd("AUTH", "default", PW) == ("ok", "OK"))
        c.close()

        c = Client(srv)
        c.s.sendall(resp_cmd("SET", "big", "x" * 20000))
        try:
            closed = c.s.recv(1024) == b""
        except ConnectionError:
            closed = True
        check("SEC-2: 20 KB inainte de AUTH deconecteaza", closed)
        c.close()

        if cli := shutil.which("redis-cli"):
            for flags in ([], ["-3"]):
                out = subprocess.run([cli, "-p", str(srv.port), *flags, "-a", PW, "--no-auth-warning", "GET", "k"],
                                     capture_output=True, timeout=10)
                check(f"SEC-2: redis-cli {' '.join(flags)} -a", out.stdout.strip() == b"v", out)
        srv.stop()

        r = run_once(srv, {"SOMNIUM_BIND": "0.0.0.0", "SOMNIUM_PASSWORD": ""})
        check("SEC-2: bind in retea fara parola refuza pornirea",
              r is not None and r.returncode == 1 and b"SOMNIUM_PASSWORD" in r.stderr, r)
    finally:
        srv.cleanup()


def sec2_no_password(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        check("SEC-2: fara parola nimic nu se schimba", c.cmd("SET", "k", "v") == ("ok", "OK"))
        check("SEC-2: fara parola AUTH e o eroare", c.cmd("AUTH", "x")[0] == "err")
        c.close()
    finally:
        srv.cleanup()


def sec2_replication(check):
    def node(node_id, port, peer_port, password):
        return Server(port=port, keep_dir_on_retry=True, env_extra={
            **ENV, "SOMNIUM_NODE_ID": str(node_id), "SOMNIUM_PEERS": f"127.0.0.1:{peer_port}",
            "SOMNIUM_PASSWORD": password})

    a, b, x = node(1, 6393, 6394, PW), node(2, 6394, 6393, PW), node(3, 6395, 6394, "wrong")
    try:
        for srv in (a, b, x):
            srv.start()
        ca, cb, cx = Client(a), Client(b), Client(x)
        for c in (ca, cb):
            c.cmd("AUTH", PW)
        cx.cmd("AUTH", "wrong")
        ca.cmd("SET", "from_a", "1")
        cx.cmd("SET", "from_x", "1")
        check("SEC-2: nodurile cu aceeasi parola replica",
              wait_until(lambda: cb.cmd("GET", "from_a") == ("bulk", b"1")))
        time.sleep(1.5)  # cateva caravane
        check("SEC-2: nodul cu parola gresita nu replica", cb.cmd("GET", "from_x")[0] == "nil")
        for c in (ca, cb, cx):
            c.close()
    finally:
        for srv in (a, b, x):
            srv.cleanup()


def sec3_file_modes(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        c.cmd("ROOM", "r")
        c.cmd("SET", "k", "v")
        c.cmd("ROOM", "default")
        check("SEC-3: ROOM.HIBERNATE", c.cmd("ROOM.HIBERNATE", "r")[0] == "ok")
        check("SEC-3: COMPACT", c.cmd("COMPACT")[0] != "err")
        c.close()
        for name in ("appendonly.aof", "room_r.bin", "cold.bin"):
            path = os.path.join(srv.workdir, name)
            mode = os.stat(path).st_mode & 0o777 if os.path.exists(path) else None
            check(f"SEC-3: {name} are modul 0600", mode == 0o600, oct(mode) if mode else "lipseste")
    finally:
        srv.cleanup()


def sec5_room_names(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        for name in ("", "a/b", "a\0b", "r" * 201):
            check(f"SEC-5: ROOM {name[:12]!r} refuzat", c.cmd("ROOM", name) == ("err", "ERR invalid room name"))
            check(f"SEC-5: ROOM.WAKE {name[:12]!r} refuzat", c.cmd("ROOM.WAKE", name)[0] == "err")
        rooms = [name for _, name in c.cmd("ROOMS")[1]]
        check("SEC-5: niciun nume invalid in registru", all(len(n) in range(1, 201) and b"/" not in n for n in rooms), rooms)

        for name in ("LIST", "SET", "r" * 200):
            check(f"SEC-5: ROOM {name[:12]!r} merge", c.cmd("ROOM", name) == ("ok", "OK") and c.cmd("SET", "k", name)[0] == "ok")
            c.cmd("ROOM", "default")
            check(f"SEC-5: ROOM.HIBERNATE {name[:12]!r}", c.cmd("ROOM.HIBERNATE", name) == ("ok", "OK"))  # bugetul de 3
        long = "r" * 200
        check("SEC-5: ...si se trezeste", c.cmd("ROOM.WAKE", long) == ("ok", "OK"))
        c.cmd("ROOM", long)
        check("SEC-5: ...cu datele intacte", c.cmd("GET", "k") == ("bulk", long.encode()))
        c.close()
    finally:
        srv.cleanup()


def sec6_maxclients(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_MAXCLIENTS": "2"})

    def admitted():
        """Un client care a primit loc; inchiderile anterioare se proceseaza asincron."""
        deadline = time.time() + 5
        while True:
            c = Client(srv)
            try:
                if c.cmd("PING") == ("ok", "PONG"):
                    return c
            except (ConnectionError, OSError):
                pass
            c.close()
            if time.time() > deadline:
                return None
            time.sleep(0.1)

    try:
        srv.start()
        a, b = admitted(), admitted()
        check("SEC-6: primii doi clienti merg", a and b)
        third = srv.connect()
        check("SEC-6: al treilea primeste eroarea si e inchis",
              third.recv(1024) == b"-ERR max number of clients reached\r\n" and third.recv(1024) == b"")
        third.close()

        a.close()
        c = admitted()
        check("SEC-6: dupa o deconectare un client nou e acceptat", c is not None)
        for x in (b, c):
            x.close()
    finally:
        srv.cleanup()


def sec7_max_rooms(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_MAX_ROOMS": "3"})
    try:
        srv.start()
        c = Client(srv)
        check("SEC-7: primele trei camere", all(c.cmd("ROOM", r) == ("ok", "OK") for r in "abc"))
        check("SEC-7: a patra camera noua e refuzata", c.cmd("ROOM", "d") == ("err", "ERR too many rooms"))
        check("SEC-7: ...si prin ROOM.WAKE", c.cmd("ROOM.WAKE", "d") == ("err", "ERR too many rooms"))
        check("SEC-7: ...fara sa intre in registru", b"d" not in [n for _, n in c.cmd("ROOMS")[1]])
        check("SEC-7: primele trei merg in continuare",
              all(c.cmd("ROOM", r) == ("ok", "OK") and c.cmd("SET", "k", r)[0] == "ok" for r in "abc"))
        c.close()
    finally:
        srv.cleanup()


def sec8_metrics(check):
    srv = Server()  # cu exporterul de metrici pornit
    try:
        srv.start()
        scrape = lambda: urllib.request.urlopen("http://127.0.0.1:9090/metrics", timeout=5).read()
        check("SEC-8: exporterul asculta pe 127.0.0.1", "127.0.0.1:9090" in server_log(srv), server_log(srv))
        check("SEC-8: scrape normal", wait_until(lambda: b"db_keys_in_ram" in scrape()))

        idle = socket.create_connection(("127.0.0.1", 9090))  # nu trimite nimic
        t0 = time.time()
        body = scrape()
        check("SEC-8: o conexiune inactiva nu blocheaza scrape-ul",
              b"db_keys_in_ram" in body and time.time() - t0 < 2, f"{time.time() - t0:.2f}s")
        idle.close()
    finally:
        srv.cleanup()


def flood(sock, seconds):
    """Trimite comenzi fara sa citeasca vreun raspuns (un client care nu urmeaza citirea)."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    sock.setblocking(False)
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            sock.send(b"a\n" * 4096)
        except BlockingIOError:
            time.sleep(0.01)
        except OSError:
            return  # serverul a inchis conexiunea


def server_connections(port):
    """Conexiunile ESTABLISHED ale serverului, vazute din kernel (fara sa citim din ele)."""
    with open("/proc/net/tcp") as f:
        rows = [line.split() for line in f.readlines()[1:]]
    return sum(r[1].endswith(f":{port:04X}") and r[3] == "01" for r in rows)


def sec17_dropped_clients(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_PASSWORD": PW})
    rss_mb = lambda: int(open(f"/proc/{srv.proc.pid}/status").read().split("VmRSS:")[1].split()[0]) >> 10
    try:
        srv.start()
        before = rss_mb()
        strangers = [srv.connect() for _ in range(10)]
        for s in strangers:
            flood(s, 1)
        grown = rss_mb() - before
        check("SEC-17: 10 straini care nu citesc nu tin memoria serverului", grown < 16, f"+{grown} MB")
        check("SEC-17: strainii sunt deconectati", wait_until(lambda: server_connections(srv.port) == 0, 5))
        for s in strangers:
            s.close()

        slow = Client(srv)
        slow.cmd("AUTH", PW)
        flood(slow.s, 20)  # pana trece de capul de 32 MB si serverul inchide; nu citeste nimic
        check("SEC-17: clientul lent deconectat chiar se inchide",
              wait_until(lambda: server_connections(srv.port) == 0, 5))
        slow.close()
    finally:
        srv.cleanup()


def sec18_auth_timeout(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_PASSWORD": PW, "SOMNIUM_MAXCLIENTS": "3"})

    def joins():
        c = Client(srv)
        try:
            return c.cmd("AUTH", PW) == ("ok", "OK")
        except ConnectionError:
            return False
        finally:
            c.close()

    try:
        srv.start()
        member = Client(srv)
        check("SEC-18: clientul autentificat intra", member.cmd("AUTH", PW) == ("ok", "OK"))
        strangers = [srv.connect(timeout=15) for _ in range(2)]  # nu trimit nimic
        t0 = time.time()
        check("SEC-18: cu locurile ocupate, un client nou e refuzat", not joins())
        check("SEC-18: dupa ~10 s strainii elibereaza locurile", wait_until(joins, 13), f"{time.time() - t0:.1f}s")
        check("SEC-18: strainii au fost deconectati", all(s.recv(1) == b"" for s in strangers))
        check("SEC-18: clientul autentificat inactiv ramane conectat", member.cmd("PING") == ("ok", "PONG"))
        for s in strangers:
            s.close()
        member.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    run_with_retry(sec1_bind, fails)
    run_with_retry(sec2_auth, fails)
    run_with_retry(sec2_no_password, fails)
    run_with_retry(sec2_replication, fails)
    run_with_retry(sec3_file_modes, fails)
    run_with_retry(sec5_room_names, fails)
    run_with_retry(sec6_maxclients, fails)
    run_with_retry(sec7_max_rooms, fails)
    run_with_retry(sec8_metrics, fails)
    run_with_retry(sec17_dropped_clients, fails)
    run_with_retry(sec18_auth_timeout, fails)
    report(fails, "sec")


if __name__ == "__main__":
    main()
