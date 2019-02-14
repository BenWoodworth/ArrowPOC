import kotlinx.serialization.KSerializer
import test.ReadWrite
import test.Serialize

class PerformanceTester(
    private val serializeServices: List<Serialize>,
    private val readWriteServices: List<ReadWrite>
) {
    data class TestResult(
        val serialFormat: String,
        val readWriteFormat: String,
        val serializeDuration: Long,
        val writeDuration: Long,
        val readDuration: Long,
        val deserializeDuration: Long,
        val serializedSize: Int
    )

    fun <T> test(testData: TestData<T>): Sequence<TestResult> {
        return sequence {
            for (serializeTester in serializeServices) {
                for (writeTester in readWriteServices) {


                            yield(
                                test(
                                    testData.data,
                                    testData.serializer,
                                    serializeTester,
                                    writeTester
                                )
                            )
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