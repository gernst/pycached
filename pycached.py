import sys
import re
import socket
import threading

CHARSET = "ascii"

ERROR = "ERROR"
STORED = "STORED"
DELETED = "DELETED"
TOUCHED = "TOUCHED"
NOT_STORED = "NOT_STORED"  # failed add, replace, prepend, append
EXISTS = "EXISTS"  # unsuccessful CAS
NOT_FOUND = "NOT_FOUND"  # bad CAS, delete

GET = "GET"
VALUE = "VALUE"
END = "END"

SERVER_ERROR = "SERVER_ERROR"
CLIENT_ERROR = "CLIENT_ERROR"

MAX_REL_TIME = 60 * 60 * 24 * 30  # 30 days
MAX_VALUE = 2**64

EOL = b"\n"


class ClientError(Exception):
    def __init__(self, message):
        self.message = message


def number(arg):
    try:
        return int(arg)
    except ValueError:
        raise ClientError("not a number")


def reltime(arg, now):
    arg = number(arg)
    if arg <= MAX_REL_TIME:
        arg += now
    return arg


class Environment:
    current_time = 0


class Entry:
    unique = 0

    def next_unique():
        Entry.unique = Entry.unique + 1
        return Entry.unique

    def __init__(self, key, flags, exptime, data):
        self.key = key
        self.flags = flags
        self.exptime = exptime
        self.data = data
        self.unique = Entry.next_unique()

    def __str__(self):
        return f"Entry({self.key}, {self.flags}, {self.exptime}, #{len(self.data)})"

    def incr(self, step):
        assert 0 <= step and step < MAX_VALUE
        value = number(self.data) + step
        if value >= MAX_VALUE:
            value -= MAX_VALUE
        value = str(value)

        self.data = value.encode(CHARSET)
        return value

    def decr(self, step):
        assert 0 <= step and step < MAX_VALUE
        value = number(self.data) - step
        if value < 0:
            value = 0
        value = str(value)

        self.data = value.encode(CHARSET)
        return value

    def touch(self, exptime):
        self.exptime = exptime

    def prepend(self, entry):
        self.data = entry.data + self.data

    def append(self, entry):
        self.data = self.data + entry.data


class Cache:
    def __init__(self):
        self.entries = {}
        self.lock = threading.Lock()

    # find a cache entry and check whether it is still active
    def find(self, key):
        if key in self.entries:
            entry = self.entries[key]
            if Environment.current_time < entry.exptime:
                return entry
            else:
                return None
        else:
            return None

    # internal operation that evicts all expired entries
    def evict_expired(self):
        keys = [key for key in self.entries if self.find(key)]

        for key in keys:
            del self.entries[key]

    def get(self, keys):
        for key in keys:
            entry = self.find(key)
            if entry:
                yield entry

    def gat(self, keys, exptime):
        for key in keys:
            entry = self.find(key)
            if entry:
                entry.touch(exptime)
                yield entry

    def set(self, key, entry):
        self.entries[key] = entry
        return STORED

    def delete(self, key):
        entry = self.find(key)
        if entry:
            del self.entries[key]
            return DELETED
        else:
            return NOT_FOUND

    def touch(self, key, exptime):
        entry = self.find(key)
        if entry:
            entry.touch(exptime)
            return TOUCHED
        else:
            return NOT_FOUND

    def incr(self, key, step):
        entry = self.find(key)
        if entry:
            return entry.incr(step)
        else:
            return NOT_FOUND

    def decr(self, key, step):
        entry = self.find(key)
        if entry:
            return entry.decr(step)
        else:
            return NOT_FOUND

    def add(self, key, entry):
        if key not in self.entries:
            self.entries[key] = entry
            return STORED
        else:
            return NOT_STORED

    def replace(self, key, entry):
        if key in self.entries:
            self.entries[key] = entry
            return STORED
        else:
            return NOT_STORED

    def cas(self, key, entry, unique):
        entry = self.find(key)
        if entry:
            if entry.unique == unique:
                self.entries[key] = entry
                return STORED
            else:
                return EXISTS
        else:
            return NOT_STORED

    # while prepend and append ignore flags and exptime
    # the command grammar nevertheless includes dummy values for these
    def prepend(self, key, entry):
        entry = self.find(key)
        if entry:
            entry.prepend(entry)
            return STORED
        else:
            return NOT_STORED

    def append(self, key, entry):
        entry = self.find(key)
        if entry:
            entry.append(entry)
            return STORED
        else:
            return NOT_STORED


class Connection(threading.Thread):
    def __init__(self, addr, input, output, cache):
        super().__init__()
        self.addr = addr
        self.input = input
        self.output = output
        self.cache = cache

        print("connected", self.addr)

    def readline(self):
        line = self.input.readline()

        if not line:
            return None
        else:
            line = line.decode(CHARSET)
            line = line.strip()
            return line

    def readdata(self, length):
        data = self.input.read(length)
        assert self.input.read(1) == EOL
        return data

    def writeline(self, *parts):
        parts = [str(part) for part in parts]
        line = " ".join(parts)
        line = line.encode(CHARSET)
        self.output.write(line)
        self.output.write(EOL)
        self.output.flush()

    def writedata(self, data):
        self.output.write(data)
        self.output.write(EOL)
        self.output.flush()

    def readentry(self, key, flags, exptime, length):
        data = self.readdata(number(length))
        entry = Entry(key, number(flags), number(exptime), data)
        return key, entry

    def writeentry(self, entry, cas):
        if cas:
            parts = [VALUE, entry.key, entry.flags, len(entry.data), entry.unique]
        else:
            parts = [VALUE, entry.key, entry.flags, len(entry.data)]

        self.writeline(*parts)
        self.writedata(entry.data)

    def run(self):
        print("serving", self.addr)

        while True:
            line = self.readline()

            if line is None:
                break

            try:
                with self.cache.lock:
                    self.handle(line)
            except ClientError as e:
                self.writeline(CLIENT_ERROR, e.message)
            # except Exception as e:
            #     self.writeline(SERVER_ERROR, "internal error")

        print("disconnected", self.addr)

    def handle(self, line):
        match line.split():
            case ["get", *keys]:
                for entry in self.cache.get(keys):
                    self.writeentry(entry, cas=False)
                self.writeline(END)

            case ["gets", *keys]:
                for entry in self.cache.get(keys):
                    self.writeentry(entry, cas=True)
                self.writeline(END)

            case ["gat", exptime, *keys]:
                for entry in self.cache.gat(keys, number(exptime)):
                    self.writeentry(entry, cas=False)
                self.writeline(END)

            case ["gats", exptime, *keys]:
                for entry in self.cache.gat(keys, number(exptime)):
                    self.writeentry(entry, cas=True)
                self.writeline(END)

            case ["set", key, flags, exptime, length]:
                key, entry = self.readentry(key, flags, exptime, length)
                result = self.cache.set(key, entry)
                self.writeline(result)

            case ["cas", key, flags, exptime, length, unique]:
                key, entry = self.readentry(key, flags, exptime, length)
                result = self.cache.cas(key, entry, number(unique))
                self.writeline(result)

            case ["delete", key]:
                result = self.cache.delete(key)
                self.writeline(result)

            case ["incr", key, step]:
                step = number(step)
                result = self.cache.incr(key, step)
                self.writeline(result)

            case ["decr", key, step]:
                step = number(step)
                result = self.cache.decr(key, step)
                self.writeline(result)

            case ["add", key, flags, exptime, length]:
                key, entry = self.readentry(key, flags, exptime, length)
                result = self.cache.add(key, entry)
                self.writeline(result)

            case ["replace", key, flags, exptime, length]:
                key, entry = self.readentry(key, flags, exptime, length)
                result = self.cache.replace(key, entry)
                self.writeline(result)

            case ["prepend", key, flags, exptime, length]:
                key, entry = self.readentry(key, flags, exptime, length)
                result = self.cache.prepend(key, entry)
                self.writeline(result)

            case ["append", key, flags, exptime, length]:
                key, entry = self.readentry(key, flags, exptime, length)
                result = self.cache.append(key, entry)
                self.writeline(result)

            case _:
                self.writeline(ERROR)


class Server(threading.Thread):
    def __init__(self, host, port):
        super().__init__()
        self.cache = Cache()
        self.addr = (host, port)

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as base:
            base.bind(self.addr)
            base.listen()

            print("listening", self.addr)

            while True:
                try:
                    conn, addr = base.accept()
                    input = conn.makefile("rb")
                    output = conn.makefile("wb")

                    client = Connection(addr, input, output, self.cache)
                    client.start()

                except KeyboardInterrupt:
                    print("terminated")
                    break


# addr = None
# cache = Cache()
# conn = Connection(addr, sys.stdin.buffer, sys.stdout.buffer, cache)
# conn.run()

server = Server("127.0.0.1", 8081)
server.run()
