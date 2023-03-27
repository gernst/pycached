from pycached.shared import Clock
from pycached.entry import Entry

STORED = "STORED"
DELETED = "DELETED"
TOUCHED = "TOUCHED"
NOT_STORED = "NOT_STORED"  # failed add, replace, prepend, append
EXISTS = "EXISTS"  # unsuccessful CAS
NOT_FOUND = "NOT_FOUND"  # bad CAS, delete


class Cache:
    # Note: all results are returned as strings,
    #       except those from get/gat

    def __init__(self):
        self.entries = {}

    # find a cache entry and check whether it is still active
    def find(self, now, key):
        if key in self.entries:
            entry = self.entries[key]
            if now < entry.exptime:
                return entry
            else:
                return None
        else:
            return None

    # internal operation that evicts all expired entries
    def evict_expired(self, now):
        keys = [key for key in self.entries if self.find(now, key)]

        for key in keys:
            del self.entries[key]

    def get(self, now, keys):
        for key in keys:
            entry = self.find(now, key)
            if entry:
                yield entry

    def gat(self, now, keys, exptime):
        for key in keys:
            entry = self.find(now, key)
            if entry:
                entry.touch(exptime)
                yield entry

    def set(self, now, key, entry):
        self.entries[key] = entry
        return STORED

    def delete(self, now, key):
        entry = self.find(now, key)
        if entry:
            del self.entries[key]
            return DELETED
        else:
            return NOT_FOUND

    def touch(self, now, key, exptime):
        entry = self.find(now, key)
        if entry:
            entry.touch(exptime)
            return TOUCHED
        else:
            return NOT_FOUND

    def incr(self, now, key, step):
        entry = self.find(now, key)
        if entry:
            return entry.incr(step)
        else:
            return NOT_FOUND

    def decr(self, now, key, step):
        entry = self.find(now, key)
        if entry:
            return entry.decr(step)
        else:
            return NOT_FOUND

    def add(self, now, key, entry):
        entry = self.find(now, key)
        if not entry:
            self.entries[key] = entry
            return STORED
        else:
            return NOT_STORED

    def replace(self, now, key, entry):
        entry = self.find(now, key)
        if entry:
            self.entries[key] = entry
            return STORED
        else:
            return NOT_STORED

    def cas(self, now, key, entry, unique):
        entry = self.find(now, key)
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
    def prepend(self, now, key, entry):
        entry = self.find(now, key)
        if entry:
            entry.prepend(entry)
            return STORED
        else:
            return NOT_STORED

    def append(self, now, key, entry):
        entry = self.find(now, key)
        if entry:
            entry.append(entry)
            return STORED
        else:
            return NOT_STORED
