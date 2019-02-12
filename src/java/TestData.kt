import kotlinx.serialization.KSerializer

class TestData<T>(
    val name: String,
    val serializer: KSerializer<T>,
    val data: T
)
