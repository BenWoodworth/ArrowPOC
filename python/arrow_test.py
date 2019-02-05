import pyarrow.plasma as plasma

def read_obj(plasma_store, obj_id):
    client = _connect(plasma_store)
    obj = client.get(obj_id)

    return obj


def write_obj(plasma_store, obj_id, obj_val):
    client = _connect(plasma_store)
    obj_id = client.put(obj_val)

    return obj_id


# Return client
def _connect(plasma_store):
    client = plasma.connect(plasma_store, "", 0)
    return client


def main():
    client = plasma.connect("/tmp/store43611", "", 0)
    # objs = client.list()
    # obj_ids = []
    # for o in objs:
    #     print(o)
    #     print(client.contains(o))
    object_id = plasma.ObjectID(20 * b"a")
    # object_size = 1000
    # buffer = memoryview(client.create(object_id, object_size))
    #
    # # Write to the buffer.
    # for i in range(1000):
    #     buffer[i] = i % 128
    #
    #
    # client.seal(object_id)

    [buffer2] = (client.get_buffers([object_id]))
    for e in buffer2:
        print(bytes(e))

    return


if __name__ == '__main__':
    main()
