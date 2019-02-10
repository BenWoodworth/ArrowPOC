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
        val deserializeDuration: Long
    )

    fun test(data: Any?): List<TestResult> {
        val result = mutableListOf<TestResult>()

        for (serializer in serializeServices) {
            for (writer in readWriteServices) {
                if (writer.platform != serializer.platform) continue
                for (reader in readWriteServices) {
                    if (reader.format != writer.format) continue
                    for (deserializer in serializeServices) {
                        if (deserializer.platform != reader.platform) continue
                        if (deserializer.format != serializer.format) continue

                        result += test(
                            data,
                            serializer,
                            writer,
                            reader,
                            deserializer
                        )
                    }
                }
            }
        }

        return result
    }

    private fun test(
        data: Any?,
        serializer: ServiceInfo<Serialize>,
        writer: ServiceInfo<ReadWrite>,
        reader: ServiceInfo<ReadWrite>,
        deserializer: ServiceInfo<Serialize>
    ): TestResult {
        lateinit var serialized: ByteArray
        lateinit var read: ByteArray

        return TestResult(
            fromPlatform = serializer.platform,
            toPlatform = deserializer.platform,
            serialFormat = serializer.format,
            readWriteFormat = reader.format,
            serializeDuration = time { serialized = serializer.service.serialize(data) },
            writeDuration = time { writer.service.write(serialized) },
            readDuration = time { read = reader.service.read() },
            deserializeDuration = time { deserializer.service.deserialize(read) }
        )
    }

    private inline fun time(action: () -> Unit): Long {
        val start = System.nanoTime()
        action()
        return System.nanoTime() - start
    }
}