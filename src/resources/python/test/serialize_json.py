import json


class SerializeJsonPy:
    class Java:
        implements = ['test.Serialize']

    def serialize(self, data: bytes) -> bytes:
        return json.dumps(data)

    def deserialize(self, data: bytes) -> bytes:
        return json.loads(data)
