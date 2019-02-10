import test.ReadWrite
import test.Serialize

interface ServiceFactory {

    fun createReadWriteFile(file: String): ReadWrite

    fun createReadWritePlasma(storePath: String): ReadWrite

    fun createSerializeJson(): Serialize

    fun createSerializeParquet(): Serialize
}