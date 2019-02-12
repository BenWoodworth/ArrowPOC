package test

import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.Cbor

class SerializeCbor : Serialize {

    private val cbor = Cbor()

    override fun <T> serialize(data: T, serializer: KSerializer<T>): ByteArray {
        return cbor.dump(serializer, data)
    }

    override fun <T> deserialize(data: ByteArray, serializer: KSerializer<T>): T {
        return cbor.load(serializer, data)
    }
}
