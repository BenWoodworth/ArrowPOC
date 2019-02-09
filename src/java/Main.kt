import java.io.File

fun main(args: Array<String>) {
    PlasmaStore(File("/tmp/plasma"), 1000).use { store ->
        val client = store.createClient()

        val id = ByteArray(20)

        client.put(id, "Hello, world!".toByteArray(), null)
        val value = client.get(id, 1000, false).toString(Charsets.UTF_8)

        println(value)
    }
}
