import pyarrow.parquet as pq


class SerializeParquet:
    def serialize(self, data):
        pq.write_table(data, 'example.parquet')
        return

    def deserialize(self, path):
        return pq.read_table(path)
