package test

interface ReadWrite {

    val format: String

    fun read(): ByteArray

    fun write(data: ByteArray)
}
