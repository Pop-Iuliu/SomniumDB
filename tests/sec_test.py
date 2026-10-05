"""Securitate (securityprogress.md): fiecare pas SEC-n isi aduce aici verificarea."""

import glob
import os
import subprocess

from common import Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}


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


def main():
    fails = []
    run_with_retry(sec1_bind, fails)
    report(fails, "sec")


if __name__ == "__main__":
    main()
