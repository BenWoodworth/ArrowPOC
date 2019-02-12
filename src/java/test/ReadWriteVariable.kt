package test

class ReadWriteVariable: ReadWrite {

    private lateinit var value: ByteArray

    override fun read(): ByteArray {
        return value
    }

    override fun write(data: ByteArray) {
        value = data
    }
}
