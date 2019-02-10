package test

interface Serialize {

    fun serialize(data: Any?): ByteArray

    fun deserialize(data: ByteArray): Any?
}
