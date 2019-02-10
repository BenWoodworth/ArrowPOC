import test.ReadWrite
import test.Serialize

@Suppress("FunctionName")
interface PythonServiceFactory {

    fun ReadWriteFilePy(file: String): ReadWrite

    fun ReadWritePlasmaPy(storePath: String, objectId: ByteArray): ReadWrite

    fun SerializeJsonPy(): Serialize

    fun SerializeParquetPy(): Serialize
}