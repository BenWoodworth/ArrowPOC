from test.readwrite_file import ReadWriteFile
from test.readwrite_plasma import ReadWritePlasma
from test.serialize_parquet import SerializeParquet
from test.serialize_json import SerializeJson


# noinspection PyPep8Naming,PyMethodMayBeStatic
class PythonServiceFactory:
    class Java:
        implements = ['ServiceFactory']

    def createReadWriteFile(self, file: str) -> ReadWriteFile:
        return ReadWriteFile(path=file)

    def createReadWritePlasma(self, store_path: str, object_id: bytes) -> ReadWritePlasma:
        return ReadWritePlasma(store_path=store_path, obj_id=object_id)

    def createSerializeJson(self) -> SerializeJson:
        return SerializeJson()

    def createSerializeParquet(self) -> SerializeParquet:
        return SerializeParquet()
