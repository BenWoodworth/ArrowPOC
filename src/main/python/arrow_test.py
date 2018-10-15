import pyarrow.plasma as plasma
import numpy as np
import pprint


def main():
    client = plasma.connect("/tmp/plasma", "", 0)
    object_id = client.put("hello, world")
    client.put(1)
    client.put(1.2)
    client.put("dsfasfjds;lkdsjflkds")
    pprint.pprint(len(client.list()))
    return

if __name__ == '__main__':
    main()