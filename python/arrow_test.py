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
    o = write_obj("/tmp/plasma", "1", "hi")
    print(o)
    x = read_obj("/tmp/plasma", o)
    print(x)
    return


if __name__ == '__main__':
    main()
