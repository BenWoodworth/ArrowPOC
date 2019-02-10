package test

import com.google.gson.Gson
import com.google.gson.JsonParser

class SerializeJson : Serialize {

    private val gson = Gson()
    private val parser = JsonParser()

    override fun serialize(data: Any?): ByteArray {
        return gson.toJson(data).toByteArray()
    }

    override fun deserialize(data: ByteArray): Any? {
        return parser.parse(String(data))
    }
}
