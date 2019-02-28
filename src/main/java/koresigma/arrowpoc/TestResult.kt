
data class TestResult(
    val serialFormat: String,
    val readWriteFormat: String,
    val serializeDuration: Long,
    val writeDuration: Long,
    val readDuration: Long,
    val deserializeDuration: Long,
    val serializedSize: Int
)

