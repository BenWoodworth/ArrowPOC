import pyarrow.plasma as plasma


class ReadWritePlasma:
    def __init__(self, store_path, obj_id):
        self.store_path = store_path
        self.obj_id = obj_id

    def read(self) -> bytes:
        client = _connect(self.store_path)
        obj = client.get(self.obj_id)
        return obj

    def write(self, data: bytes):
        client = _connect(self.store_path)
        client.put(data, self.obj_id)
        return


# Write to the Plasma Store without having a predefined object ID
# Stores the object and returns it's (new) ID
def write_plasma_without_id(store_path: str, data: object) -> bytes:
    client = _connect(store_path)
    obj_id = client.put(data)
    return obj_id


# Helper function for code reuse
# Returns the client
def _connect(store_path):
    client = plasma.connect(store_path, "", False)
    return client
