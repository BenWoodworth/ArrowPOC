package test

import PlasmaStore

class ReadWritePlasma(
    private val store: PlasmaStore,
    private val objectId: ByteArray
): ReadWrite {

    private val client = store.createClient()

    override fun read(): ByteArray {
        return client.get(objectId, 1000, false)
    }

    override fun write(data: ByteArray) {
        if (client.contains(objectId)) {
            client.release(objectId)
        }

        client.put(objectId, data, null)
    }
}
