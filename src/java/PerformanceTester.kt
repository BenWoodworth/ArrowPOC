import kotlinx.serialization.KSerializer
import test.ReadWrite
import test.Serialize

class PerformanceTester(
    private val serializeServices: List<ServiceInfo<Serialize>>,
    private val readWriteServices: List<ServiceInfo<ReadWrite>>
) {
    data class TestResult(
        val fromPlatform: String,
        val toPlatform: String,
        val serialFormat: String,
        val readWriteFormat: String,
        val serializeDuration: Long,
        val writeDuration: Long,
        val readDuration: Long,
        val deserializeDuration: Long,
        val serializedSize: Int
    )

    fun <T> test(testData: TestData<T>): List<TestResult> {
        val result = mutableListOf<TestResult>()

        for (serializeTester in serializeServices) {
            for (writeTester in readWriteServices) {
                if (writeTester.platform != serializeTester.platform) continue
                for (readTester in readWriteServices) {
                    if (readTester.format != writeTester.format) continue
                    for (deserializeTester in serializeServices) {
                        if (deserializeTester.platform != readTester.platform) continue
                        if (deserializeTester.format != serializeTester.format) continue

                        result += test(
                            testData.data,
                            testData.serializer,
                            serializeTester,
                            writeTester,
                            readTester,
                            deserializeTester
                        )
                    }
                }
            }
        }

        return result
    }

    private fun <T> test(
        data: T,
        serializer: KSerializer<T>,
        serializeTester: ServiceInfo<Serialize>,
        writeTester: ServiceInfo<ReadWrite>,
        readTester: ServiceInfo<ReadWrite>,
        deserializeTester: ServiceInfo<Serialize>
    ): TestResult {
        lateinit var serialized: ByteArray
        lateinit var read: ByteArray

        return TestResult(
            fromPlatform = serializeTester.platform,
            toPlatform = deserializeTester.platform,
            serialFormat = serializeTester.format,
            readWriteFormat = readTester.format,
            serializeDuration = time { serialized = serializeTester.service.serialize(data, serializer) },
            writeDuration = time { writeTester.service.write(serialized) },
            readDuration = time { read = readTester.service.read() },
            deserializeDuration = time { deserializeTester.service.deserialize(read, serializer) },
            serializedSize = serialized.size
        )
    }

    private inline fun time(action: () -> Unit): Long {
        val start = System.nanoTime()
        action()
        return System.nanoTime() - start
    }
}