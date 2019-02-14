package test

import kotlinx.serialization.KSerializer

interface Serialize {

    val format: String

    fun <T> serialize(data: T, serializer: KSerializer<T>): ByteArray

    fun <T> deserialize(data: ByteArray, serializer: KSerializer<T>): T
}
