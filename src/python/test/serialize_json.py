import json


class SerializeJson:
    def serialize(self, data: bytes) -> bytes:
        return json.dumps(data)

    def deserialize(self, data: bytes) -> bytes:
        return json.loads(data)
