package koresigma.arrowpoc.test

import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf

class SerializeProtoBuf : Serialize {

    override val format: String = "protobuf"

    private val protoBuf = ProtoBuf()

    override fun <T> serialize(data: T, serializer: KSerializer<T>): ByteArray {
        return protoBuf.dump(serializer, data)
    }

    override fun <T> deserialize(data: ByteArray, serializer: KSerializer<T>): T {
        return protoBuf.load(serializer, data)
    }
}
