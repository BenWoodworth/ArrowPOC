import pyarrow.parquet as pq


def serialize(data):
    pq.write_table(data, 'example.parquet')
    return


def deserialize(path):
    return pq.read_table(path)
