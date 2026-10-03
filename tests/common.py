"""Utilitare comune pentru suitele de teste SomniumDB (stdlib only)."""

import os
import signal
import socket
import subprocess
import shutil
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Server:
    """Cicleaza de viata al serverului intr-un director de date curat."""

    def __init__(self, bin_path=None, port=None, env_extra=None, keep_dir_on_retry=False):
        self.bin = os.path.abspath(
            bin_path
            or os.environ.get("SOMNIUM_BIN")
            or os.path.join(REPO, "build-release", "Redis")
        )
        self.port = int(port or os.environ.get("REDIS_PORT", "6380"))
        self.workdir = tempfile.mkdtemp(prefix="somnium-test-")
        self.proc = None
        self._log = None
        self.env_extra = dict(env_extra or {})
        # testele care insamneaza fisiere in director NU trebuie sa-si piarda
        # datele la o reincercare de start (flake-ul io_uring)
        self.keep_dir_on_retry = keep_dir_on_retry

    def start(self, retries=3):
        """Porneste serverul; reuse pana la `retries` incercari.

        Pe acest kernel (6.8.0-138) exista o anomalie de mediu: ocazional
        (~1/30 porniri rapide) io_uring nu livreaza nicio completare (acceptul
        si poll-urile raman in aer desi socketul asculta si erau conexiuni in
        coada). Procesul nou asemanator functioneaza, deci reincercarea rezolva.
        """
        last_err = None
        for attempt in range(retries):
            # portul trebuie sa fie legabil efectiv: un server "flaked" dar in
            # viu de la o incercare anterioara il poate tine ocupat (bind esuat
            # => proces mort inca de la start => conexiuni refuzate)
            deadline = time.time() + 3.0
            while time.time() < deadline:
                probe = socket.socket()
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    probe.bind(("0.0.0.0", self.port))
                    probe.close()
                    break
                except OSError:
                    probe.close()
                    time.sleep(0.1)
            env = dict(os.environ, REDIS_PORT=str(self.port), **self.env_extra)
            self._log = open(os.path.join(self.workdir, f"server.{attempt}.log"), "wb")
            self.proc = subprocess.Popen(
                [self.bin], cwd=self.workdir, env=env,
                stdout=self._log, stderr=subprocess.STDOUT,
            )
            try:
                self.wait_ready()
                return
            except (RuntimeError, OSError) as e:
                last_err = e
                self.stop()
                # director curat pentru urmatoarea incercare (exceptie: testele
                # cu fisiere insamnate manual pastreaza directorul)
                if not self.keep_dir_on_retry:
                    shutil.rmtree(self.workdir, ignore_errors=True)
                    os.makedirs(self.workdir, exist_ok=True)
        raise RuntimeError(f"serverul nu a pornit dupa {retries} incercari: {last_err}")

    def wait_port_free(self, timeout=5.0):
        """Asteapta ca portul sa fie eliberat efectiv (socketul vechi inchis).

        Sonda prinde SO_REUSEADDR: starea TIME_WAIT lasata in urma conexiunilor
        ucise (flake-ul io_uring) nu blocheaza bind-ul unui listener nou, deci
        nu trebuie sa blocheze nici sonda.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            probe = socket.socket()
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("0.0.0.0", self.port))
                probe.close()
                return
            except OSError:
                probe.close()
                time.sleep(0.1)
        raise RuntimeError(f"portul {self.port} nu s-a eliberat in {timeout}s")

    def restart(self):
        self.stop()
        # lasam kernelul sa elibereze socketul de listen inainte de rebind
        self.wait_port_free()
        self.start()

    def wait_ready(self, timeout=15.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                self.dump_log("serverul a murit in timpul pornirii")
                raise RuntimeError(f"serverul nu a pornit pe portul {self.port}")
            try:
                s = self.connect(timeout=1.0)
                s.close()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError(f"serverul nu a pornit pe portul {self.port}")

    def connect(self, timeout=5.0):
        s = socket.create_connection(("127.0.0.1", self.port), timeout=timeout)
        s.settimeout(timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return s

    def stop(self):
        if not self.proc:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.communicate()
        self._log.close() if self._log and not self._log.closed else None

    def dump_log(self, reason):
        """Afiseaza coada ultimului log de server (debug in CI)."""
        print(f"--- [somnium] {reason} - server log tail ---")
        try:
            logs = sorted(
                os.path.join(self.workdir, f) for f in os.listdir(self.workdir)
                if f.startswith("server") and f.endswith(".log")
            )
            if logs:
                with open(logs[-1], "rb") as f:
                    print(f.read().decode(errors="replace")[-2000:])
        except OSError as e:
            print("log ilizibil:", e)

    def cleanup(self):
        self.stop()
        if self.proc and self.proc.returncode not in (None, 0, -15, -2):
            self.dump_log(f"procesul a murit cu codul {self.proc.returncode}")
        if self._log and not self._log.closed:
            self._log.close()
        shutil.rmtree(self.workdir, ignore_errors=True)


def resp_cmd(*args):
    raw = b"*" + str(len(args)).encode() + b"\r\n"
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        raw += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"
    return raw


def recv_until(s, want_crlf, want_bytes=0, timeout=10.0):
    """Citeste pana la un numar de CRLF-uri si/sau minim de bytes."""
    s.settimeout(timeout)
    data = b""
    while data.count(b"\r\n") < want_crlf or len(data) < want_bytes:
        chunk = s.recv(1 << 20)
        if not chunk:
            break
        data += chunk
    return data


class Client:
    """Client RESP minimal cu buffer propriu: un raspuns per comanda, array-uri incluse."""

    def __init__(self, srv):
        self.s = srv.connect(timeout=10)
        self.buf = b""

    def cmd(self, *args):
        self.s.sendall(resp_cmd(*args))
        return self.read_reply()

    def _fill(self):
        chunk = self.s.recv(1 << 16)
        if not chunk:
            raise ConnectionError("serverul a inchis conexiunea")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def read_reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return ("ok", rest.decode())
        if t == b"-":
            return ("err", rest.decode())
        if t == b":":
            return ("int", int(rest))
        if t == b"*":
            return ("array", [self.read_reply() for _ in range(int(rest))])
        # RESP3: null distinct de $-1, harti si mesaje push
        if t == b"_":
            return ("null", None)
        if t == b"%":
            return ("map", [(self.read_reply(), self.read_reply()) for _ in range(int(rest))])
        if t == b">":
            return ("push", [self.read_reply() for _ in range(int(rest))])
        if t == b"$":
            n = int(rest)
            if n == -1:
                return ("nil", None)
            while len(self.buf) < n + 2:
                self._fill()
            payload, self.buf = self.buf[:n], self.buf[n + 2:]
            return ("bulk", payload)
        raise AssertionError(f"raspuns necunoscut: {line!r}")

    def close(self):
        self.s.close()


def run_with_retry(fn, fails, max_attempts=3):
    """Ruleaza o sectiune de test, reluata de la zero la decesul serverului.

    Flake-ul io_uring documentat in AGENTS.md poate livra un server fara
    nicio completare; sectiunile sunt self-contained (Server curat de fiecare
    data), deci le putem re-rula. Esecurile reale de asertie raman consemnate
    la ultima incercare.
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
                print(f"  (sectiune reluata dupa {type(e).__name__})")


def report(fails, suite):
    print(f"\n[{suite}] {len(fails)} esecuri", fails if fails else "")
    sys.exit(1 if fails else 0)
