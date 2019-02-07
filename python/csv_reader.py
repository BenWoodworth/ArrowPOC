from pyarrow import csv
import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np

client = plasma.connect("/tmp/store", "", 0)

table = csv.read_csv('../data/baseball.csv')
record_batch = table.to_batches()[0]
object_id = plasma.ObjectID(np.random.bytes(20))
mock_sink = pa.MockOutputStream()
stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
stream_writer.write_batch(record_batch)
stream_writer.close()
data_size = mock_sink.size()
buf = client.create(object_id, data_size)


stream = pa.FixedSizeBufferWriter(buf)
stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
stream_writer.write_batch(record_batch)
stream_writer.close()
client.seal(object_id)

print(object_id)


# table_dict_id = client.put(table_dict)
# print(table_dict_id)

# object_id = plasma.ObjectID(20 * b"a")
# object_size = len(table)
# buffer = memoryview(client.create(object_id, object_size))
#
# # Write to the buffer.
# for i in range(object_size):
#     buffer[i] = bytes(table[i])
