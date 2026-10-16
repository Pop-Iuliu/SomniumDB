"""Refactorizare (refactorprogress.md): pasii care schimba comportamentul isi aduc aici verificarea."""

import os
import subprocess

from common import Client, Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}


def ref1_failed_start(check):
    srv = Server(env_extra={**ENV, "SOMNIUM_MAX_KEYS": "0"})  # orice cheie ajunge in cold storage
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "k", "v")
        cold = os.path.join(srv.workdir, "cold.bin")
        size = os.path.getsize(cold)
        check("REF-1: cheia e in cold.bin", size > 0)

        # o a doua pornire gresita in acelasi director nu are voie sa atinga datele
        for name, extra in (("acelasi port", {}), ("configuratie invalida", {"SOMNIUM_BIND": "not-an-ip"})):
            env = dict(os.environ, REDIS_PORT=str(srv.port), **ENV, **extra)
            r = subprocess.run([srv.bin], cwd=srv.workdir, env=env, capture_output=True, timeout=10)
            check(f"REF-1: a doua pornire ({name}) iese cu codul 1", r.returncode == 1, r)
            check(f"REF-1: ...fara sa atinga cold.bin ({name})", os.path.getsize(cold) == size, os.path.getsize(cold))

        check("REF-1: prima instanta isi citeste in continuare cheia rece", c.cmd("GET", "k") == ("bulk", b"v"))
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    run_with_retry(ref1_failed_start, fails)
    report(fails, "ref")


if __name__ == "__main__":
    main()
