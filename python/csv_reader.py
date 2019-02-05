from pyarrow import csv
from pyarrow import Schema
import pyarrow.plasma as plasma

table = csv.read_csv('../data/baseball.csv')
table_dict = table.to_pydict()
client = plasma.connect("/tmp/store", "", 0)
table_dict_id = client.put(table_dict)
print(table_dict_id)

# object_id = plasma.ObjectID(20 * b"a")
# object_size = len(table)
# buffer = memoryview(client.create(object_id, object_size))
#
# # Write to the buffer.
# for i in range(object_size):
#     buffer[i] = bytes(table[i])
