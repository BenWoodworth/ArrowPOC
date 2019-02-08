import pyarrow.plasma as plasma


def read_plasma(store_path, obj_id):
    client = _connect(store_path)
    obj = client.get(obj_id)
    return obj


def write_plasma(store_path, obj_id, obj_val):
    client = _connect(store_path)
    client.put(obj_val, obj_id)
    return


# Write to the Plasma Store without having a predefined object ID
# Stores the object and returns it's (new) ID
def write_plasma_without_id(store_path, obj_val):
    client = _connect(store_path)
    obj_id = client.put(obj_val)
    return obj_id


# Helper function for code reuse
# Returns the client
def _connect(store_path):
    client = plasma.connect(store_path, "", False)
    return client
