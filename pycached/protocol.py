from pycached.shared import to_number, ClientError
from pycached.entry import Entry

MAX_REL_TIME = 60 * 60 * 24 * 30  # 30 days


# used on str from command parts
# placed here because this is clearly a protocol-level feature
def to_unixtime(arg, now):
    arg = to_number(arg)
    if arg <= MAX_REL_TIME:
        arg += now
    return arg


class Protocol:
    def __init__(self, conn, clock, cache):
        self.conn = conn
        self.clock = clock
        self.cache = cache

    def read_entry(self, now, key, flags, exptime, length):
        flags = to_number(flags)
        exptime = to_unixtime(exptime, now)
        length = to_number(length)

        data = self.conn.read_data(length)
        entry = Entry(key, flags, exptime, data)

        return key, entry

    def write_end(self):
        self.conn.write_cmd("END")

    def write_result(self, result):
        self.conn.write_cmd(result)

    def write_error(self):
        self.conn.write_cmd("ERROR")

    def write_client_error(self, message):
        self.conn.write_cmd("CLIENT_ERROR", message)

    def write_server_error(self, message):
        self.conn.write_cmd("SERVER_ERROR", message)

    def write_entry(self, entry, cas):
        key = entry.key
        flags = str(entry.flags)
        # exptime is not printed
        length = str(len(entry.data))
        unique = str(entry.unique)

        if cas:
            parts = ["VALUE", key, flags, length, unique]
        else:
            parts = ["VALUE", key, flags, length]

        self.conn.write_cmd(*parts)
        self.conn.write_data(entry.data)

    def serve(self):
        while True:
            cmd = self.conn.read_cmd()

            if cmd is None:
                break

            try:
                now = self.clock.current_unixtime()
                self.handle(now, cmd)
            except ClientError as e:
                self.write_client_error(e.message)
            # except Exception as e:
            #     self.write_server_error("internal error")

    def handle(self, now, cmd):
        # Note: command names are case sensitive
        match cmd:
            case ["get", *keys]:
                for entry in self.cache.get(now, keys):
                    self.write_entry(entry, cas=False)
                self.write_end()

            case ["gets", *keys]:
                for entry in self.cache.get(now, keys):
                    self.write_entry(entry, cas=True)
                self.write_end()

            case ["gat", exptime, *keys]:
                exptime = to_unixtime(exptime, clock)
                for entry in self.cache.gat(now, keys, exptime):
                    self.write_entry(entry, cas=False)
                self.write_end()

            case ["gats", exptime, *keys]:
                exptime = to_unixtime(exptime)
                for entry in self.cache.gat(now, keys, exptime):
                    self.write_entry(entry, cas=True)
                self.write_end()

            case ["set", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                result = self.cache.set(now, key, entry)
                self.write_result(result)

            case ["cas", key, flags, exptime, length, unique]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                unique = to_number(unique)
                result = self.cache.cas(now, key, entry, unique)
                self.write_result(result)

            case ["delete", key]:
                result = self.cache.delete(now, key)
                self.write_result(result)

            case ["incr", key, step]:
                step = to_number(step)
                result = self.cache.incr(now, key, step)
                self.write_result(result)

            case ["decr", key, step]:
                step = to_number(step)
                result = self.cache.decr(now, key, step)
                self.write_result(result)

            case ["add", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                result = self.cache.add(now, key, entry)
                self.write_result(result)

            case ["replace", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                result = self.cache.replace(now, key, entry)
                self.write_result(result)

            case ["prepend", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                result = self.cache.prepend(now, key, entry)
                self.write_result(result)

            case ["append", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                result = self.cache.append(now, key, entry)
                self.write_result(result)

            case _:
                self.write_error()
