package test

import java.io.File

class ReadWriteFile(private val file: File): ReadWrite {

    override val format: String = "file"

    override fun read(): ByteArray {
        return file.readBytes()
    }

    override fun write(data: ByteArray) {
        file.writeBytes(data)
    }
}
