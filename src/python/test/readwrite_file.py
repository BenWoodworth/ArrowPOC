class ReadWriteFile:
    def __init__(self, path):
        self.path = path

    def read(self):
        f = open(self.path, 'rb')
        contents = f.read()
        f.close()
        return contents

    def write(self, byte_array):
        f = open(self.path, 'wb')
        f.write(byte_array)
        f.close()
