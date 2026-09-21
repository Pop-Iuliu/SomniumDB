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

    def __init__(self, bin_path=None, port=None):
        self.bin = os.path.abspath(
            bin_path
            or os.environ.get("SOMNIUM_BIN")
            or os.path.join(REPO, "build-release", "Redis")
        )
        self.port = int(port or os.environ.get("REDIS_PORT", "6380"))
        self.workdir = tempfile.mkdtemp(prefix="somnium-test-")
        self.proc = None
        self._log = None

    def start(self, retries=3):
        """Porneste serverul; reuse pana la `retries` incercari.

        Pe acest kernel (6.8.0-138) exista o anomalie de mediu: ocazional
        (~1/30 porniri rapide) io_uring nu livreaza nicio completare (acceptul
        si poll-urile raman in aer desi socketul asculta si erau conexiuni in
        coada). Procesul nou asemanator functioneaza, deci reincercarea rezolva.
        """
        last_err = None
        for attempt in range(retries):
            env = dict(os.environ, REDIS_PORT=str(self.port))
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
                # director curat pentru urmatoarea incercare
                shutil.rmtree(self.workdir, ignore_errors=True)
                os.makedirs(self.workdir, exist_ok=True)
        raise RuntimeError(f"serverul nu a pornit dupa {retries} incercari: {last_err}")

    def wait_port_free(self, timeout=5.0):
        """Asteapta ca portul sa fie eliberat efectiv (socketul vechi inchis)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            probe = socket.socket()
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


def report(fails, suite):
    print(f"\n[{suite}] {len(fails)} esecuri", fails if fails else "")
    sys.exit(1 if fails else 0)
