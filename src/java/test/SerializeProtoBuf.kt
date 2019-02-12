package test

import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf

class SerializeProtoBuf : Serialize {

    override fun <T> serialize(data: T, serializer: KSerializer<T>): ByteArray {
        return ProtoBuf.dump(serializer, data)
    }

    override fun <T> deserialize(data: ByteArray, serializer: KSerializer<T>): T {
        return ProtoBuf.load(serializer, data)
    }
}
