from pycached.shared import to_number, ClientError
from pycached.entry import Entry


# used on str from command parts
# placed here because this is clearly a protocol-level feature
MAX_REL_TIME = 60 * 60 * 24 * 30  # 30 days


def to_unixtime(arg, now):
    arg = to_number(arg)
    if arg <= MAX_REL_TIME:
        arg += now
    return arg


class Protocol:
    def __init__(self, conn, clock, lock, cache):
        self.conn = conn
        self.clock = clock
        self.lock = lock
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
                self.handle(cmd)
            except ClientError as e:
                self.write_client_error(e.message)
            # except Exception as e:
            #     self.write_server_error("internal error")

    def handle(self, cmd):
        # Notes:
        # - Command names are case sensitive
        # - A bit of redundancy in the different cases is accepted in favor of clarity of control flow
        # - Locking is done around cache access but not during communication
        # - Current time is measured once upfront
        #   which means we can keep time dependencies out of the cache,
        #   and we have a well-defined notion of where time can advance
        # - Note, putting the clock into the cache does not work,
        #   because relative time stamps are clearly a protocol-level
        #   feature that should not be part of the cache module,
        #   such that *this* code needs access to the current time anyway
        #   (arguably, this is a design bug in memcached).
        # - In memcached, timestamps are relative to when the command is parsed,
        #   (is this good?) which is another indication that relative time should be computed here,

        #   For example
        #       set x 0 5 1
        #   wait for more than 5s, then enter data
        #       X
        #   then immediately
        #       get x
        #   returns nothing because x is already stale

        now = self.clock.current_unixtime()

        match cmd:
            case ["get", *keys]:
                with self.lock:
                    entries = list(self.cache.get(now, keys))

                for entry in entries:
                    self.write_entry(entry, cas=False)
                self.write_end()

            case ["gets", *keys]:
                with self.lock:
                    entries = list(self.cache.get(now, keys))

                for entry in entries:
                    self.write_entry(entry, cas=True)
                self.write_end()

            case ["gat", exptime, *keys]:
                with self.lock:
                    exptime = to_unixtime(exptime, now)
                    entries = list(self.cache.gat(now, keys, exptime))

                for entry in entries:
                    self.write_entry(entry, cas=False)
                self.write_end()

            case ["gats", exptime, *keys]:
                with self.lock:
                    exptime = to_unixtime(exptime, now)
                    entries = list(self.cache.gat(now, keys, exptime))

                for entry in entries:
                    self.write_entry(entry, cas=True)
                self.write_end()

            case ["set", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                with self.lock:
                    result = self.cache.set(now, key, entry)
                self.write_result(result)

            case ["cas", key, flags, exptime, length, unique]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                unique = to_number(unique)
                with self.lock:
                    result = self.cache.cas(now, key, entry, unique)
                self.write_result(result)

            case ["delete", key]:
                with self.lock:
                    result = self.cache.delete(now, key)
                self.write_result(result)

            case ["incr", key, step]:
                step = to_number(step)
                with self.lock:
                    result = self.cache.incr(now, key, step)
                self.write_result(result)

            case ["decr", key, step]:
                step = to_number(step)
                with self.lock:
                    result = self.cache.decr(now, key, step)
                self.write_result(result)

            case ["add", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                with self.lock:
                    result = self.cache.add(now, key, entry)
                self.write_result(result)

            case ["replace", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                with self.lock:
                    result = self.cache.replace(now, key, entry)
                self.write_result(result)

            case ["prepend", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                with self.lock:
                    result = self.cache.prepend(now, key, entry)
                self.write_result(result)

            case ["append", key, flags, exptime, length]:
                key, entry = self.read_entry(now, key, flags, exptime, length)
                with self.lock:
                    result = self.cache.append(now, key, entry)
                self.write_result(result)

            case _:
                self.write_error()
