class ReadWriteFile:
    class Java:
        implements = ['test.ReadWrite']

    def __init__(self, path: str):
        self.path = path

    def read(self) -> bytes:
        f = open(self.path, 'rb')
        contents = f.read()
        f.close()
        return contents

    def write(self, data: bytes):
        f = open(self.path, 'wb')
        f.write(data)
        f.close()
