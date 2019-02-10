from test.readwrite_file import ReadWriteFile
from test.readwrite_plasma import ReadWritePlasma
from test.serialize_parquet import SerializeParquet
from test.serialize_json import SerializeJson


# noinspection PyPep8Naming,PyMethodMayBeStatic
class PythonServiceFactory:
    class Java:
        implements = ['PythonServiceFactory']

    def ReadWriteFilePy(self, file: str) -> ReadWriteFile:
        return ReadWriteFile(path=file)

    def ReadWritePlasmaPy(self, store_path: str, object_id: bytes) -> ReadWritePlasma:
        return ReadWritePlasma(store_path=store_path, obj_id=object_id)

    def SerializeJsonPy(self) -> SerializeJson:
        return SerializeJson()

    def SerializeParquetPy(self) -> SerializeParquet:
        return SerializeParquet()
