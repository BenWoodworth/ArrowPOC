import pyarrow.parquet as pq


class SerializeParquet:
    class Java:
        implements = ['test.Serialize']

    def serialize(self, data: bytes) -> bytes:
        pq.write_table(file, 'example.parquet')
        return

    def deserialize(self, data: bytes) -> bytes:
        return pq.read_table(path)
