import json


def serialize_json(data):
    return json.dumps(data)


def deserialize_json(json_string):
    return json.loads(json_string)
