from pyarrow import csv as arrow_csv
import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
import time
import csv
import json
import os


def json_testing():
    write_file = 'millions.json'

    # Delete file if already exists
    if os.path.exists(write_file):
        os.remove(write_file)

    csv_file = open('../data/million.csv', 'r')
    json_file = open(write_file, 'w')

    fieldnames = csv_file.readline().strip('\n').split(",")
    reader = csv.DictReader(csv_file, fieldnames)
    out = json.dumps([row for row in reader])

    # write to json_file
    json_file.write(out)

    # reading back from json_file
    json_data = open(write_file).read()
    data = json.loads(json_data)


def stream_testing():
    # connect to plasma
    client = plasma.connect("/tmp/store", "", 0)

    # csv -> table -> record batch
    table = arrow_csv.read_csv('../data/million.csv')
    record_batch = table.to_batches()[0]

    # create an object id
    object_id = plasma.ObjectID(np.random.bytes(20))

    # record batch -> stream writer
    mock_sink = pa.MockOutputStream()
    stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
    stream_writer.write_batch(record_batch)
    stream_writer.close()

    # create buffer in plasma client
    data_size = mock_sink.size()
    buf = client.create(object_id, data_size)

    # stream writer -> write to plasma buffer
    stream = pa.FixedSizeBufferWriter(buf)
    stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
    stream_writer.write_batch(record_batch)
    stream_writer.close()

    client.seal(object_id)

    # ----------------Reading Data back from plasma----------------------------

    # Get PlasmaBuffer from ObjectID
    [data] = client.get_buffers([object_id])
    buffer = pa.BufferReader(data)

    # Plasmabuffer -> record batch
    reader = pa.RecordBatchStreamReader(buffer)
    record_batch = reader.read_next_batch()

    # record batch -> python dictionary
    py_dict = record_batch.to_pydict()


def test_data():
    times = []
    for i in range(0, 10):
        start = time.time()
        # stream_testing()
        # json_testing()
        end = time.time()
        times.append(end - start)

    i = 0
    while i < len(times):
        print("Trail #{trail}: {time}".format(trail=i, time=times[i]))
        i += 1

    avg_time = sum(times) / len(times)
    print("\nAverage: {average}".format(average=avg_time))


test_data()
