import pyarrow.parquet as pq


def serialize_parquet(data):
    pq.write_table(data, 'example.parquet')
    return


def deserialize_parquet(path):
    return pq.read_table(path)
