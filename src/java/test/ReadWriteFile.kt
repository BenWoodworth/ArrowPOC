package test

import java.nio.file.Path

class ReadWriteFile(val file: Path): ReadWrite {

    override fun read(): ByteArray {
        TODO("not implemented")
    }

    override fun write(data: ByteArray) {
        TODO("not implemented")
    }
}
