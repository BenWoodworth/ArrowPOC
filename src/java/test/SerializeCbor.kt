package test

import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.Cbor

class SerializeCbor : Serialize {

    override fun <T> serialize(data: T, serializer: KSerializer<T>): ByteArray {
        return Cbor.dump(serializer, data)
    }

    override fun <T> deserialize(data: ByteArray, serializer: KSerializer<T>): T {
        return Cbor.load(serializer, data)
    }
}
