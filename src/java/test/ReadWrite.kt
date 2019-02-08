package test

interface ReadWrite {

    fun read(): ByteArray

    fun write(data: ByteArray)
}
