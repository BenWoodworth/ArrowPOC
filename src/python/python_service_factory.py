from test.readwrite_file import ReadWriteFile
from test.readwrite_plasma import ReadWritePlasma
from test.serialize_parquet import SerializeParquet
from test.serialize_json import SerializeJson


class PythonServiceFactory:
    class Java:
        implements = ['ServiceFactory']

    def createReadWriteFile(self, file: str) -> ReadWriteFile:
        return ReadWriteFile(path=file)

    def createReadWritePlasma(self, storePath: str, objectId: str) -> ReadWritePlasma:
        return ReadWritePlasma(store_path=storePath, obj_id=objectId)

    def createSerializeJson(self) -> SerializeJson:
        return SerializeJson()

    def createSerializeParquet(self) -> SerializeParquet:
        return SerializeParquet()
