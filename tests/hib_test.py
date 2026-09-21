"""Test hibernare: snapshot v2, trezire, si supravietuirea istoricului CRDT."""

import time

from common import Server, resp_cmd, recv_until, report


def main():
    fails = []

    def check(name, cond, extra=""):
        print(("PASS" if cond else "FAIL"), name, extra if not cond else "")
        if not cond:
            fails.append(name)

    srv = Server()
    try:
        srv.start()
        s = srv.connect()
        s.sendall(resp_cmd("ROOM", "hibroom"))
        recv_until(s, 1)
        s.sendall(resp_cmd("SET", "hk", "hibval"))
        check("SET hk", b"+OK" in recv_until(s, 1))
        # ts ridicat ca sa verificam ca snapshot-ul isi aminteste istoricul
        s.sendall(resp_cmd("CRDTMERGE", "ck", "crdtval", "424242", "42"))
        check("CRDTMERGE ck", b"Converged" in recv_until(s, 1))
        s.close()

        print("astept 17s pentru watchdog (idle > 10s)...")
        time.sleep(17)

        s = srv.connect()
        s.sendall(resp_cmd("ROOM", "hibroom"))
        recv_until(s, 1)
        s.sendall(resp_cmd("GET", "hk"))
        check("trezire din snapshot v2: hk", b"hibval" in recv_until(s, 2))
        s.sendall(resp_cmd("CRDTMERGE", "ck", "STALE", "1", "1"))
        check("istoric CRDT a supravietuit hibernarii", b"Ignored" in recv_until(s, 1))
        s.sendall(resp_cmd("GET", "ck"))
        check("valoarea ck intacta", b"crdtval" in recv_until(s, 2))
        s.close()

        # restart: acelasi director de date, fara AOF -> doar snapshot
        srv.restart()
        s = srv.connect()
        s.sendall(resp_cmd("ROOM", "hibroom"))
        recv_until(s, 1)
        s.sendall(resp_cmd("GET", "hk"))
        check("snapshot dupa restart: hk", b"hibval" in recv_until(s, 2))
        s.sendall(resp_cmd("CRDTMERGE", "ck", "STALE", "1", "1"))
        check("CRDT ts a supravietuit restartului", b"Ignored" in recv_until(s, 1))
        s.close()
    finally:
        srv.cleanup()

    report(fails, "hibernare")


if __name__ == "__main__":
    main()
