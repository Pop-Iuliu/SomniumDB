"""S12 "indulgently": un peer inaccesibil nu mai blocheaza replicarea.

Verifica: cu un peer care inghite pachetele (192.0.2.1, TEST-NET-1) primul in
SOMNIUM_PEERS, un peer sanatos primeste scrierile in cateva secunde (inainte,
connect-ul blocant tinea thread-ul unic ~2 minute); INFO raporteaza phi si
lag-ul fiecarui peer.
"""

import re
import time

from common import Client, Server, report, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}
PORT_A, PORT_B = 6391, 6392
BLACKHOLE = "192.0.2.1:6399"


def peers(client):
    info = client.cmd("INFO")[1].decode()
    return {m[0]: (float(m[1]), int(m[2])) for m in re.findall(r"Peer (\S+): phi=(\S+) lag=(\d+)", info)}


def wait_until(cond, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.2)
    return cond()


def sec_isolation(check):
    a = Server(port=PORT_A, keep_dir_on_retry=True, env_extra={
        **ENV, "SOMNIUM_NODE_ID": "1", "SOMNIUM_PEERS": f"{BLACKHOLE},127.0.0.1:{PORT_B}"})
    b = Server(port=PORT_B, keep_dir_on_retry=True, env_extra={**ENV, "SOMNIUM_NODE_ID": "2"})
    try:
        b.start()
        a.start()
        ca, cb = Client(a), Client(b)
        ca.cmd("SET", "k", "v")
        check("peer-ul sanatos primeste scrierea in ciuda celui inaccesibil",
              wait_until(lambda: cb.cmd("GET", "k") == ("bulk", b"v")))

        healthy = f"127.0.0.1:{PORT_B}"
        check("INFO raporteaza ambii peers", wait_until(lambda: {healthy, BLACKHOLE} <= set(peers(ca))), peers(ca))
        check("peer-ul sanatos: phi mic si lag zero",
              wait_until(lambda: peers(ca)[healthy][0] < 3 and peers(ca)[healthy][1] == 0), peers(ca))
        p = peers(ca)[BLACKHOLE]
        check("peer-ul inaccesibil: phi infinit si lag pozitiv", p[0] == float("inf") and p[1] > 0, p)
        ca.close()
        cb.close()
    finally:
        a.cleanup()
        b.cleanup()


def main():
    fails = []
    run_with_retry(sec_isolation, fails)
    report(fails, "s12")


if __name__ == "__main__":
    main()
