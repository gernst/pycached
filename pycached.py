import sys
import re

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
            return int(data)
        except ValueError:
            raise ClientError("cannot increment or decrement non-numeric value")

    def incr(self, step):
        assert 0 <= step and step < MAX_VALUE
        value = self.value() + step
        if value >= MAX_VALUE:
            value -= MAX_VALUE
        self.data = str(value)

    def decr(self, step):
        assert 0 <= step and step < MAX_VALUE
        value = self.value() + step
        if value < 0:
            value = 0
        self.data = str(value)

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
            self.entries.remove(key)
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
            entry.incr(step)
            return entry.data
        else:
            return NOT_FOUND

    def decr(self, key, step):
        if key in self.entries:
            entry = self.entries[key]
            entry.decr(step)
            return entry.data
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


class Server:
    def __init__(self, input, output):
        self.input = input
        self.output = output
        self.cache = Cache()
    
    def readline(self):
        line = self.input.readline()
        line = line.decode("ascii")
        line = line.strip()
        return line

    def readdata(self, length):
        data = self.input.read(length)
        assert self.input.read(1) == EOL
        return data
    
    def writeline(self, *parts):
        parts = [str(part) for part in parts]
        line = " ".join(parts)
        line = line.encode("ascii")
        self.output.write(line)
        self.output.write(EOL)
        self.output.flush()

    def writedata(self, data):
        self.output.write(data)
        self.output.write(EOL)
        self.output.flush()
    
    def run(self):
        while True:
            line = self.readline()

            match line.split():
                case ["get", *keys]:
                    for entry in self.cache.get(keys):
                        self.writeline(VALUE, entry.key, entry.flags, entry.exptime, len(entry.data))
                        self.writedata(entry.data)
                    self.writeline(END)

                case ["gets", *keys]:
                    for entry in self.cache.get(keys):
                        self.writeline(VALUE, entry.key, entry.flags, entry.exptime, len(entry.data), entry.unique)
                        self.writedata(entry.data)
                    self.writeline(END)
                

                case ["gat", exptime, *keys]:
                    exptime = int(exptime)

                    for entry in self.cache.gat(keys, exptime):
                        self.writeline(VALUE, entry.key, entry.flags, entry.exptime, len(entry.data))
                        self.writedata(entry.data)
                    self.writeline(END)

                case ["gats", exptime, *keys]:
                    exptime = int(exptime)

                    for entry in self.cache.gat(keys, exptime):
                        self.writeline(VALUE, entry.key, entry.flags, entry.exptime, len(entry.data), entry.unique)
                        self.writedata(entry.data)
                    self.writeline(END)

                case ["set", key, flags, exptime, length]:
                    flags = int(flags)
                    exptime = int(exptime)
                    length = int(length)
                    data = self.readdata(length)

                    entry = Entry(key, flags, exptime, data)
                    result = self.cache.set(key, entry)

                    self.writeline(result)

                case _:
                    self.writeline(ERROR)


server = Server(sys.stdin.buffer, sys.stdout.buffer)
server.run()
