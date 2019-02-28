package koresigma.arrowpoc.test

class ReadWriteVariable: ReadWrite {

    override val format: String = "variable"

    private lateinit var value: ByteArray

    override fun read(): ByteArray {
        return value
    }

    override fun write(data: ByteArray) {
        value = data
    }
}
