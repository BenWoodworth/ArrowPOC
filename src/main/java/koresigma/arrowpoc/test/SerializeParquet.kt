package koresigma.arrowpoc.test

import kotlinx.serialization.KSerializer

class SerializeParquet : Serialize {

    override val format: String = "parquet"

    override fun <T> serialize(data: T, serializer: KSerializer<T>): ByteArray {
        TODO("not implemented")
    }

    override fun <T> deserialize(data: ByteArray, serializer: KSerializer<T>): T {
        TODO("not implemented")
    }
}
