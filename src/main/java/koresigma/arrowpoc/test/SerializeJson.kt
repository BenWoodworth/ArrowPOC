package koresigma.arrowpoc.test

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

class SerializeJson : Serialize {

    override val format: String = "json"

    override fun <T> serialize(data: T, serializer: KSerializer<T>): ByteArray {
        return Json.stringify(serializer, data).toByteArray()
    }

    override fun <T> deserialize(data: ByteArray, serializer: KSerializer<T>): T {
        return Json.parse(serializer, String(data))
    }
}
