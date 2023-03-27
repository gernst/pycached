from pycached.shared import str_to_bytes, bytes_to_str

EOL = b"\n"


class Connection:
    def __init__(self, addr, input, output):
        self.addr = addr
        self.input = input
        self.output = output

    def read_cmd(self):
        line = self.input.readline()

        if not line:
            return None
        else:
            line = bytes_to_str(line)
            line = line.strip()
            return line.split()

    def read_data(self, length):
        data = self.input.read(length)
        assert self.input.read(len(EOL)) == EOL
        return data

    def write_cmd(self, *parts):
        line = str_to_bytes(" ".join(parts))
        self.output.write(line)
        self.output.write(EOL)
        self.output.flush()

    def write_data(self, data):
        self.output.write(data)
        self.output.write(EOL)
        self.output.flush()
