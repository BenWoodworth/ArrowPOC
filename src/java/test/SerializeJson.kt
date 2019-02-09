package test

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

class SerializeJson<T>(
    private val serializer: KSerializer<T>
) : Serialize<T> {

    private val json = Json()

    override fun serialize(data: T): ByteArray {
        return json.stringify(serializer, data).toByteArray()
    }

    override fun deserialize(data: ByteArray): T {
        return json.parse(serializer, String(data))
    }
}
