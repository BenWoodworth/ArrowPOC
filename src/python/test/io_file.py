def read_file(path):
    f = open(path, 'rb')
    contents = f.read()
    f.close()
    return contents


def write_file(path, byte_array):
    f = open(path, 'wb')
    f.write(byte_array)
    f.close()
