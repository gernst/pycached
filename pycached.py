import sys
import socket
import threading
import time

from pycached.shared import Clock
from pycached.cache import Cache
from pycached.connection import Connection
from pycached.protocol import Protocol

class LocalClock(Clock):
    def current_unixtime(self):
        return int(time.time())

def run_local():
    addr = None
    clock = LocalClock()
    cache = Cache()
    conn = Connection(addr, sys.stdin.buffer, sys.stdout.buffer)
    proto = Protocol(conn, clock, cache)

    print("serving (local)")
    proto.serve()

def run_server(host, port):
    addr = (host, port)
    clock = LocalClock()
    cache = Cache()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as base:
        base.bind(addr)
        base.listen()

        print("listening", addr)

        while True:
            try:
                conn, addr = base.accept()
                input = conn.makefile("rb")
                output = conn.makefile("wb")

                conn = Connection(addr, input, output)
                proto = Protocol(conn, clock, cache)


                def run():
                    print("serving", addr)
                    proto.serve()
                    print("disconnected", addr)

                thread = threading.Thread(target = run)
                thread.start()

            except KeyboardInterrupt:
                print("terminated")
                break


# run_local()
run_server("localhost", 1234)