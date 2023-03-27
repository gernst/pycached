from pycached.shared import to_number, number_to_str_and_bytes

MAX_VALUE = 2**64

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

    def touch(self, exptime):
        # note: touching does *not* bump the version
        self.exptime = exptime

    # NOTE: do not modify entries descrtructively without bumping the version
        
    def prepend(self, entry):
        self.data = entry.data + self.data

    def append(self, entry):
        self.data = self.data + entry.data

    def incr(self, step):
        assert 0 <= step and step < MAX_VALUE
        value = to_number(self.data) + step
        if value >= MAX_VALUE:
            value -= MAX_VALUE
        value, data = number_to_str_and_bytes(value)
        self.data = data
        return value

    def decr(self, step):
        assert 0 <= step and step < MAX_VALUE
        value = to_number(self.data) - step
        if value < 0:
            value = 0
        value, data = number_to_str_and_bytes(value)
        self.data = data
        return value