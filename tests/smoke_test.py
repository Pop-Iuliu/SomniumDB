"""Smoke test: comenzi de baza, pubsub, recovery AOF dupa restart."""

import sys

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

        s.sendall(resp_cmd("SET", "foo", "bar"))
        check("SET", b"+OK" in recv_until(s, 1))
        s.sendall(resp_cmd("GET", "foo"))
        r = recv_until(s, 2)
        check("GET roundtrip", b"$3\r\nbar" in r, r)
        s.sendall(resp_cmd("GET", "missing"))
        check("GET miss -> nil", b"$-1" in recv_until(s, 1))
        s.sendall(resp_cmd("ROOM", "roomA"))
        check("ROOM switch", b"+OK" in recv_until(s, 1))
        s.sendall(resp_cmd("SET", "x", "y"))
        check("SET in roomA", b"+OK" in recv_until(s, 1))

        # CRDTMERGE cu input invalid trebuie sa intoarca -ERR, nu sa lase serverul jos
        s.sendall(resp_cmd("CRDTMERGE", "k", "v", "notanumber", "1"))
        r = recv_until(s, 1)
        check("CRDTMERGE bad input -> -ERR", r.startswith(b"-ERR"), r)
        s.sendall(resp_cmd("CRDTMERGE", "k", "v2", "999999999", "7"))
        check("CRDTMERGE valid", b"Converged" in recv_until(s, 1))
        s.sendall(resp_cmd("CRDTMERGE", "k", "old", "5", "7"))
        check("CRDTMERGE stale ignorat", b"Ignored" in recv_until(s, 1))

        s.sendall(resp_cmd("INFO"))
        check("INFO", b"Camere active" in recv_until(s, 2))
        s.sendall(resp_cmd("DEL", "x"))
        check("DEL", b":1" in recv_until(s, 1))
        s.close()

        # pubsub
        sub = srv.connect()
        sub.sendall(resp_cmd("SUBSCRIBE", "news"))
        recv_until(sub, 3)
        pub = srv.connect()
        pub.sendall(resp_cmd("PUBLISH", "news", "salut"))
        check("PUBLISH 1 receptor", b":1" in recv_until(pub, 1))
        got = recv_until(sub, 3)
        check("abonatul primeste mesajul", b"salut" in got, got)
        sub.close()
        pub.close()

        # client nou ajunge in default
        s = srv.connect()
        s.sendall(resp_cmd("GET", "foo"))
        check("default pastreaza foo", b"bar" in recv_until(s, 2))
        s.close()

        # restart: AOF recovery cu prefix de camera
        srv.restart()
        s = srv.connect()
        s.sendall(resp_cmd("GET", "foo"))
        check("AOF recovery: foo", b"bar" in recv_until(s, 2))
        s.sendall(resp_cmd("GET", "x"))
        check("AOF recovery: DEL respectat", b"$-1" in recv_until(s, 1))
        s.close()
    finally:
        srv.cleanup()

    report(fails, "smoke")


if __name__ == "__main__":
    main()
