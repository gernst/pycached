import sys
import socket
import threading
import time

from pycached.shared import Clock
from pycached.cache import Cache
from pycached.connection import Connection
from pycached.protocol import Protocol


class LocalClock(Clock):
    # real-world time!
    # alternatives that could drive this code may be based on some logical time or some explicit stepping
    def current_unixtime(self):
        return int(time.time())


def run_local():
    addr = None
    clock = LocalClock()
    lock = threading.Lock()
    cache = Cache()
    conn = Connection(addr, sys.stdin.buffer, sys.stdout.buffer)
    proto = Protocol(conn, clock, lock, cache)

    print("serving (local)")
    proto.serve()


def run_server(host, port):
    addr = (host, port)

    # shared state between the threads
    clock = LocalClock()
    lock = threading.Lock()
    cache = Cache()

    # spin up the server for the specified hostname and port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as base:
        base.bind(addr)
        base.listen()

        print("listening", addr)

        while True:
            try:
                # accept connection and turn socket
                # into a pair of file-like objects for reading and writing
                conn, addr = base.accept()
                input = conn.makefile("rb")
                output = conn.makefile("wb")

                # instantiate the protocol handler
                conn = Connection(addr, input, output)
                proto = Protocol(conn, clock, lock, cache)

                def run():
                    print("serving", addr)
                    proto.serve()
                    print("disconnected", addr)

                # spawn of new thread that executes proto.serve
                thread = threading.Thread(target=run)
                thread.start()

            except KeyboardInterrupt:
                print("terminated")
                break

match sys.argv:
    case [bin]:
        run_local()
    case [bin, host, port]:
        run_server(host, int(port))
