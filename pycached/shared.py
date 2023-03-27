CHARSET = "ascii"

def bytes_to_str(arg):
    return arg.decode(CHARSET)

def str_to_bytes(arg):
    return arg.encode(CHARSET)

# used on bytes by incr/decr
# otherwise used on str from command parts
def to_number(arg):
    try:
        return int(arg)
    except ValueError:
        raise ClientError("not a number")

def number_to_str_and_bytes(arg):
    s = str(arg)
    b = str_to_bytes(s)
    return s, b # s is needed to format the response, b is stored internally

class ClientError(Exception):
    def __init__(self, message):
        self.message = message

class Clock:
    def current_unixtime(self):
        raise NotImplementedError("please subclass Clock to define a time source")