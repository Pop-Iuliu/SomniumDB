"""S6 "hyaenanche": input otravit si abonati care ar fi putut ingheta serverul.

Verifica: inputul malformat primeste o eroare de protocol si inchide doar
conexiunea vinovata; o comanda trimisa octet cu octet se executa exact o
data; un abonat impins peste capul de output e deconectat fara ca publisherul
sa ramana blocat (inainte, remove_client() relua mutexul tinut de publish()).
"""

import time

from common import Client, Server, report, resp_cmd, run_with_retry

ENV = {"SOMNIUM_NO_METRICS": "1"}


def read_to_eof(s, timeout=10):
    s.settimeout(timeout)
    data = b""
    while True:
        chunk = s.recv(1 << 16)
        if not chunk:
            return data
        data += chunk


def sec_protocol_errors(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        healthy = Client(srv)
        for name, payload in (
            ("lungime negativa", b"*1\r\n$-1\r\n"),
            ("lungime uriasa", b"*1\r\n$99999999999\r\n"),
            ("numar negativ de argumente", b"*-1\r\n"),
            ("bulk fara CRLF", b"*1\r\n$3\r\nabcXY"),
            ("comanda inline (pana la S7)", b"PING\r\n"),
        ):
            bad = srv.connect()
            bad.sendall(payload)
            got = read_to_eof(bad)
            bad.close()
            check(f"{name}: eroare de protocol si conexiune inchisa", got == b"-ERR Protocol error\r\n", got)
            check(f"{name}: ceilalti clienti merg in continuare",
                  healthy.cmd("SET", "alive", name) == ("ok", "OK"))

        bad = srv.connect()
        bad.sendall(resp_cmd("SET", "before", "1") + b"*-1\r\n" + resp_cmd("SET", "after", "1"))
        got = read_to_eof(bad)
        bad.close()
        check("comenzile dinaintea erorii raspund, cele de dupa nu se executa",
              got == b"+OK\r\n-ERR Protocol error\r\n", got)
        check("comanda dinainte s-a aplicat", healthy.cmd("GET", "before") == ("bulk", b"1"))
        check("comanda de dupa nu s-a aplicat", healthy.cmd("GET", "after")[0] == "nil")
        healthy.close()
    finally:
        srv.cleanup()


def sec_byte_by_byte(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        c = Client(srv)
        raw = resp_cmd("SET", "split", "v\r\nx") + resp_cmd("GET", "split")
        for i in range(len(raw)):
            c.s.sendall(raw[i:i + 1])
            time.sleep(0.002)  # citiri separate pe server, nu un singur pachet
        check("SET trimis octet cu octet: un singur raspuns", c.read_reply() == ("ok", "OK"))
        check("GET trimis octet cu octet", c.read_reply() == ("bulk", b"v\r\nx"))
        c.close()
    finally:
        srv.cleanup()


def sec_slow_subscriber(check):
    srv = Server(env_extra=ENV)
    try:
        srv.start()
        sub = srv.connect()
        sub.sendall(resp_cmd("SUBSCRIBE", "ch"))
        time.sleep(0.2)  # abonatul nu mai citeste nimic de aici incolo

        pub = Client(srv)
        msg = "M" * (1 << 20)
        replies = []
        for _ in range(64):  # 64MB: peste capul de 32MB plus bufferele kernelului
            replies.append(pub.cmd("PUBLISH", "ch", msg))
        check("publisherul primeste fiecare raspuns (fara blocaj)", len(replies) == 64)
        check("abonatul lent a fost deconectat si scos din canal", replies[-1] == ("int", 0), replies[-1])
        check("serverul raspunde in continuare", pub.cmd("SET", "after", "1") == ("ok", "OK"))
        pub.close()
        sub.close()
    finally:
        srv.cleanup()


def main():
    fails = []
    for sec in (sec_protocol_errors, sec_byte_by_byte, sec_slow_subscriber):
        run_with_retry(sec, fails)
    report(fails, "s6")


if __name__ == "__main__":
    main()
