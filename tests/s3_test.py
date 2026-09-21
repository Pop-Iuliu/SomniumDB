"""S3 acceptance: output nebloctant, fair-share, pubsub cu abonati lenti."""

import sys
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

        # 1) client lent (nu citeste) nu trebuie sa blocheze un client rapid
        big_value = "B" * 300000  # 100 x 300KB = 30MB pe care clientul lent nu le citeste
        slow = srv.connect()
        fast = srv.connect()
        slow.sendall(b"".join(resp_cmd("SET", f"big:{i}", big_value) for i in range(100)))
        time.sleep(0.3)
        slow.sendall(b"".join(resp_cmd("GET", f"big:{i}") for i in range(100)))

        latencies = []
        for _ in range(50):
            t1 = time.time()
            fast.sendall(resp_cmd("SET", "fair:key", "fairval"))
            r = recv_until(fast, 1)
            latencies.append(time.time() - t1)
            if not r.startswith(b"+OK"):
                break
        mx = max(latencies)
        check("clientul rapid ramane functioneaza cu client lent blocat", mx < 2.0, f"max={mx*1000:.0f}ms")
        print(f"  latencies: avg={sum(latencies)/len(latencies)*1000:.1f}ms max={mx*1000:.1f}ms")

        # 2) raspunsuri mari, byte-exact si ordonate
        # 100 +OK (1 CRLF) + 100 raspunsuri bulk (2 CRLF) = 300 CRLF
        drain = recv_until(slow, 300, timeout=30)
        ok_gets = drain.count(b"$300000\r\n")
        check("raspunsuri mari complete si ordonate (100/300KB)", ok_gets == 100,
              f"primit {len(drain)} bytes, raspunsuri {ok_gets}/100")

        # 3) abonat lent nu blocheaza publisherul
        sub = srv.connect()
        sub.sendall(resp_cmd("SUBSCRIBE", "ch"))
        recv_until(sub, 3)
        pub = srv.connect()
        msg = "M" * 100000
        pub_lat = []
        for _ in range(60):
            t2 = time.time()
            pub.sendall(resp_cmd("PUBLISH", "ch", msg))
            r = recv_until(pub, 1)
            pub_lat.append(time.time() - t2)
            if not r.startswith(b":1"):
                break
        pmax = max(pub_lat)
        check("publisher ramane rapid cu abonat lent", pmax < 2.0, f"max={pmax*1000:.0f}ms")
        print(f"  publish latencies: avg={sum(pub_lat)/len(pub_lat)*1000:.1f}ms max={pmax*1000:.1f}ms")

        # 4) pipeline buffered se termina singur (eventfd pump)
        big = srv.connect()
        big.sendall(b"".join(resp_cmd("SET", f"pipe:{i}", f"val-{i}") for i in range(3000)))
        buf = recv_until(big, 3000, timeout=10)
        check("pipeline buffered se termina singur (eventfd pump)",
              buf.count(b"+OK") == 3000, f"got {buf.count(b'+OK')}/3000")

        for s in (slow, fast, sub, pub, big):
            s.close()
    finally:
        srv.cleanup()

    report(fails, "S3")


if __name__ == "__main__":
    main()
