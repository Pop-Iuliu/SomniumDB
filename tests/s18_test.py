"""S18 "operational": fdatasync everysec in afara lock-ului de append.

Verifica: o intarziere injectata in fdatasync nu se mosteneste de SET sub
everysec; octetii scrisi in timpul sincronizarii raman pending pana la
sincronizarea urmatoare; o completare de pe generatia veche nu marcheaza
AOF-ul nou dupa REWRITEAOF; un snapshot lent pe watchdog nu amana incercarea
de sync; sub always, raspunsul asteapta sincronizarea.
"""

import time
import urllib.request

from common import Client, Server, report, resp_cmd, run_with_retry

PORT = 6401


def metrics():
    body = urllib.request.urlopen("http://127.0.0.1:9090/metrics", timeout=5).read().decode()
    out = {}
    for ln in body.splitlines():
        if not ln or ln.startswith("#"):
            continue
        key, value = ln.split()
        if value.lstrip("-").isdigit():
            out[key] = int(value)
    return out


def wait_until(cond, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def sec_everysec_does_not_block_append(check):
    srv = Server(port=PORT, env_extra={
        "SOMNIUM_AOF_SYNC": "everysec",
        "SOMNIUM_AOF_SYNC_DELAY_MS": "250",
        "SOMNIUM_AOF_SYNC_INTERVAL_MS": "200",
    })
    try:
        srv.start()
        c = Client(srv)
        caught = None
        for n in range(8):
            c.cmd("SET", "warm", str(n))
            deadline = time.time() + 1.5
            while time.time() < deadline:
                if metrics().get("db_aof_sync_inflight") == 1:
                    t0 = time.time()
                    c.cmd("SET", "during", "v")
                    lat = time.time() - t0
                    m = metrics()
                    caught = (lat, m.get("db_aof_pending_bytes", 0), m.get("db_aof_append_lock_waits_slow", 0))
                    break
                time.sleep(0.005)
            if caught and caught[0] < 0.15 and caught[1] > 0 and caught[2] == 0:
                break
        check("SET in timpul fdatasync nu mosteneste intarzierea de 250ms",
              caught is not None and caught[0] < 0.15, caught)
        check("lock-ul de append nu a fost tinut peste 50ms", caught is not None and caught[2] == 0, caught)
        check("scrierea din timpul sync ramane pending", caught is not None and caught[1] > 0, caught)
        check("pending se acopera la sincronizarea urmatoare",
              wait_until(lambda: metrics().get("db_aof_pending_bytes") == 0, 3), metrics())
        c.close()
        srv.restart()
        c = Client(srv)
        check("valoarea sincronizata supravietuieste restartului", c.cmd("GET", "during") == ("bulk", b"v"))
        c.close()
    finally:
        srv.cleanup()


def sec_always_waits(check):
    srv = Server(port=PORT, env_extra={
        "SOMNIUM_AOF_SYNC": "always",
        "SOMNIUM_AOF_SYNC_DELAY_MS": "250",
    })
    try:
        srv.start()
        c = Client(srv)
        t0 = time.time()
        r = c.cmd("SET", "ack", "yes")
        lat = time.time() - t0
        print(f"  always SET: {lat * 1000:.0f} ms")
        check("always: SET reuseste", r == ("ok", "OK"), r)
        check("always: raspunsul asteapta fdatasync", lat >= 0.20, f"{lat * 1000:.0f} ms")
        check("always: nimic pending dupa ack", metrics().get("db_aof_pending_bytes") == 0, metrics())
        c.close()
        srv.restart()
        c = Client(srv)
        check("always: valoarea e pe disc", c.cmd("GET", "ack") == ("bulk", b"yes"))
        c.close()
    finally:
        srv.cleanup()


def sec_old_generation_cannot_clean_new_file(check):
    srv = Server(port=PORT, env_extra={
        "SOMNIUM_AOF_SYNC": "everysec",
        "SOMNIUM_AOF_SYNC_DELAY_MS": "2000",
        "SOMNIUM_AOF_SYNC_INTERVAL_MS": "8000",
    })
    try:
        srv.start()
        c = Client(srv)
        value = "v" * 64
        commands = [resp_cmd("SET", "k", value + str(i)) for i in range(200)]
        c.s.sendall(b"".join(commands))
        for _ in commands:
            c.read_reply()

        large_before_sync = False
        inflight = False
        deadline = time.time() + 12
        while time.time() < deadline:
            m = metrics()
            if m.get("db_aof_sync_inflight") == 0 and m.get("db_aof_bytes", 0) > 8000:
                large_before_sync = True
            if m.get("db_aof_sync_inflight") == 1 and large_before_sync:
                inflight = True
                break
            time.sleep(0.02)
        check("sincronizarea prinde un AOF mare", inflight, metrics())
        gen = metrics().get("db_aof_generation")

        check("REWRITEAOF", c.cmd("REWRITEAOF")[0] == "ok")
        done = wait_until(lambda: b"in curs" not in c.cmd("INFO")[1], 5)
        check("rescrierea se termina in fereastra de sync", done)
        check("generatia creste la publicarea fisierului nou", metrics().get("db_aof_generation", 0) > gen, metrics())

        c.cmd("SET", "after", "new")
        m = metrics()
        check("completarea veche nu sterge pending-ul noii generatii", m.get("db_aof_pending_bytes", 0) > 0, m)
        check("offsetul vechi nu depaseste fisierul nou", m.get("db_aof_synced_bytes", 0) <= m.get("db_aof_bytes", 0), m)

        check("dupa completare, generatia noua ramane consistenta",
              wait_until(lambda: metrics().get("db_aof_sync_inflight") == 0, 4))
        m = metrics()
        check("synced nu sare peste dimensiunea noii generatii",
              m.get("db_aof_synced_bytes", 0) <= m.get("db_aof_bytes", 0), m)
        c.close()
    finally:
        srv.cleanup()


def sec_snapshot_does_not_postpone_sync(check):
    srv = Server(port=PORT, env_extra={
        "SOMNIUM_AOF_SYNC": "everysec",
        "SOMNIUM_AOF_SYNC_INTERVAL_MS": "200",
        "SOMNIUM_SNAPSHOT_DELAY_MS": "1500",
    })
    try:
        srv.start()
        c = Client(srv)
        check("camera idle", c.cmd("ROOM", "idle")[0] == "ok")
        c.cmd("SET", "x", "y")
        c.cmd("ROOM", "default")

        ages = []
        deadline = time.time() + 22
        hibernated = False
        while time.time() < deadline:
            c.cmd("SET", "tick", "1")
            info = c.cmd("ROOM.INFO", "idle")
            state = None
            if info[0] == "array" and len(info[1]) > 1:
                state = info[1][1][1]
            age = metrics().get("db_aof_sync_age_ms", -1)
            if state == b"hibernating" and age >= 0:
                ages.append(age)
            if state == b"sleeping" and ages:
                hibernated = True
                break
            time.sleep(0.05)
        print(f"  varste in timpul snapshot-ului: n={len(ages)} max={max(ages) if ages else None}")
        check("snapshot-ul lent a rulat", hibernated and len(ages) >= 3, (hibernated, ages[:8]))
        check("varsta sync ramane sub intarzierea snapshot-ului", bool(ages) and max(ages) < 900, ages)
        c.close()
    finally:
        srv.cleanup()


def sec_overrun_metric(check):
    srv = Server(port=PORT, env_extra={
        "SOMNIUM_AOF_SYNC": "everysec",
        "SOMNIUM_AOF_SYNC_DELAY_MS": "200",
        "SOMNIUM_AOF_SYNC_INTERVAL_MS": "50",
    })
    try:
        srv.start()
        c = Client(srv)
        c.cmd("SET", "o", "1")
        check("un sync mai lung decat intervalul numara overrun",
              wait_until(lambda: metrics().get("db_aof_sync_overruns", 0) >= 1, 3), metrics())
        c.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_everysec_does_not_block_append, sec_always_waits, sec_overrun_metric,
                sec_old_generation_cannot_clean_new_file, sec_snapshot_does_not_postpone_sync):
        run_with_retry(sec, fails)
    report(fails, "s18")


if __name__ == "__main__":
    main()
