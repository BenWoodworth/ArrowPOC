from test.readwrite_file import ReadWriteFilePy
from test.readwrite_plasma import ReadWritePlasmaPy
from test.serialize_parquet import SerializeParquetPy
from test.serialize_json import SerializeJsonPy


# noinspection PyPep8Naming,PyMethodMayBeStatic
class PythonServiceFactory:
    class Java:
        implements = ['PythonServiceFactory']

    def ReadWriteFilePy(self, file: str) -> ReadWriteFilePy:
        return ReadWriteFilePy(path=file)

    def ReadWritePlasmaPy(self, store_path: str, object_id: bytes) -> ReadWritePlasmaPy:
        return ReadWritePlasmaPy(store_path=store_path, obj_id=object_id)

    def SerializeJsonPy(self) -> SerializeJsonPy:
        return SerializeJsonPy()

    def SerializeParquetPy(self) -> SerializeParquetPy:
        return SerializeParquetPy()
