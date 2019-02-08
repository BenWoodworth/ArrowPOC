import json


class SerializeJson:
    def serialize(self, data):
        return json.dumps(data)

    def deserialize(self, json_string):
        return json.loads(json_string)
