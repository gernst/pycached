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

CLIENT_ERROR = "CLIENT_ERROR"

MAX_VALUE = 2**64

EOL = b"\n"


class ClientError(Exception):
    def __init__(self, message):
        self.message = message


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
        return f"Entry({self.key}, {self.flags}, {self.exptime}, {len(self.data)})"

    def value(self):
        try:
            return int(self.data)
        except ValueError:
            raise ClientError("cannot increment or decrement non-numeric value")

    def incr(self, step):
        assert 0 <= step and step < MAX_VALUE
        value = self.value() + step
        if value >= MAX_VALUE:
            value -= MAX_VALUE
        value = str(value)

        self.data = value.encode(CHARSET)
        return value

    def decr(self, step):
        assert 0 <= step and step < MAX_VALUE
        value = self.value() - step
        if value < 0:
            value = 0
        value = str(value)

        self.data = value.encode(CHARSET)
        return value

    def touched(self, exptime):
        self.exptime = exptime
        return self  # used in Cache.gat

    def prepend(self, entry):
        self.data = entry.data + self.data

    def append(self, entry):
        self.data = self.data + entry.data


class Cache:
    def __init__(self):
        self.entries = {}

    def get(self, keys):
        return [self.entries[key] for key in keys if key in self.entries]

    def gat(self, keys, exptime):
        return [
            self.entries[key].touched(exptime) for key in keys if key in self.entries
        ]

    def set(self, key, entry):
        self.entries[key] = entry
        return STORED

    def delete(self, key):
        if key in self.entries:
            del self.entries[key]
            return DELETED
        else:
            return NOT_FOUND

    def touch(self, key, exptime):
        if key in self.entries:
            self.entries.touched(exptime)
            return TOUCHED
        else:
            return NOT_FOUND

    def incr(self, key, step):
        if key in self.entries:
            entry = self.entries[key]
            return entry.incr(step)
        else:
            return NOT_FOUND

    def decr(self, key, step):
        if key in self.entries:
            entry = self.entries[key]
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
        if key in self.entries:
            if self.entries[key].unique == unique:
                self.entries[key] = entry
                return STORED
            else:
                return EXISTS
        else:
            return NOT_STORED

    # while prepend and append ignore flags and exptime
    # the command grammar nevertheless includes dummy values for these
    def prepend(self, key, entry):
        if key in self.entries:
            self.entries[key].prepend(entry)
            return STORED
        else:
            return NOT_STORED

    def append(self, key, entry):
        if key in self.entries:
            self.entries[key].append(entry)
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
        data = self.readdata(int(length))
        entry = Entry(key, int(flags), int(exptime), data)
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
                self.handle(line)
            except ClientError as e:
                self.writeline(CLIENT_ERROR, e.message)
        
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
                for entry in self.cache.gat(keys, int(exptime)):
                    self.writeentry(entry, cas=False)
                self.writeline(END)

            case ["gats", exptime, *keys]:
                for entry in self.cache.gat(keys, int(exptime)):
                    self.writeentry(entry, cas=True)
                self.writeline(END)

            case ["set", key, flags, exptime, length]:
                key, entry = self.readentry(key, flags, exptime, length)
                result = self.cache.set(key, entry)
                self.writeline(result)

            case ["cas", key, flags, exptime, length, unique]:
                key, entry = self.readentry(key, flags, exptime, length)
                result = self.cache.cas(key, entry, int(unique))
                self.writeline(result)

            case ["delete", key]:
                result = self.cache.delete(key)
                self.writeline(result)

            case ["incr", key, step]:
                step = int(step)
                result = self.cache.incr(key, step)
                self.writeline(result)

            case ["decr", key, step]:
                step = int(step)
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

            while True:
                conn, addr = base.accept()
                input = conn.makefile("rb")
                output = conn.makefile("wb")

                client = Connection(addr, input, output, self.cache)
                client.start()


# conn = Connection(sys.stdin.buffer, sys.stdout.buffer)
# conn.run()

server = Server("127.0.0.1", 8081)
server.run()
