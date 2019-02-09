package test

interface Serialize<T> {

    fun serialize(data: T): ByteArray

    fun deserialize(data: ByteArray): T
}
