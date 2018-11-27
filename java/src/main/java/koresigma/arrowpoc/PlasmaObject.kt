package koresigma.arrowpoc

import org.apache.arrow.plasma.PlasmaClient

class PlasmaObject(
    private val client: PlasmaClient,
    private val objectId: ByteArray
) {

    val hash: ByteArray
        get() = client.hash(this.objectId)

    val exists: Boolean
        get() = client.contains(this.objectId)

    fun setBytes(bytes: ByteArray, metadata: ByteArray = byteArrayOf()) {
        client.put(this.objectId, bytes, metadata)
    }

    fun getBytes(timeoutMs: Int? = null): ByteArray {
        return client.get(this.objectId, timeoutMs ?: -1, false)
    }

    fun getMetadata(timeoutMs: Int? = null): ByteArray {
        return client.get(this.objectId, timeoutMs ?: -1, true)
    }

    fun fetch() {
        client.fetch(this.objectId)
    }

    fun release() {
        client.release(this.objectId)
    }
}
