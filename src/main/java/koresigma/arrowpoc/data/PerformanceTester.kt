import koresigma.arrowpoc.data.TestData
import kotlinx.serialization.KSerializer
import test.ReadWrite
import test.Serialize

class PerformanceTester(
    private val serializeServices: List<Serialize>,
    private val readWriteServices: List<ReadWrite>
) {
    fun <T> test(testData: TestData<T>): Sequence<TestResult> {
        val data = testData.getData()

        return sequence {
            for (serializeTester in serializeServices) {
                for (writeTester in readWriteServices) {
                    yield(test(data, testData.serializer, serializeTester, writeTester))
                }
            }
        }
    }

    private fun <T> test(
        data: T,
        serializer: KSerializer<T>,
        serializeService: Serialize,
        readWriteService: ReadWrite
    ): TestResult {
        lateinit var serialized: ByteArray
        lateinit var read: ByteArray

        return TestResult(
            serialFormat = serializeService.format,
            readWriteFormat = readWriteService.format,
            serializeDuration = time { serialized = serializeService.serialize(data, serializer) },
            writeDuration = time { readWriteService.write(serialized) },
            readDuration = time { read = readWriteService.read() },
            deserializeDuration = time { serializeService.deserialize(read, serializer) },
            serializedSize = serialized.size
        )
    }

    private inline fun time(action: () -> Unit): Long {
        val start = System.nanoTime()
        action()
        return System.nanoTime() - start
    }
}