import py4j.GatewayServer
import test.ReadWritePlasma
import java.io.File

val readWriters = listOf(
    PerformanceTester.ServiceInfo("jvm", "file", ReadWritePlasma)
)

fun main(args: Array<String>) {

    val entryPoint = PythonEntryPoint { pySerializers, pyReadWriters ->

        PlasmaStore(File("/tmp/plasma"), 1000).use { store ->
            val client = store.createClient()

            val id = ByteArray(20)

            client.put(id, "Hello, world!".toByteArray(), null)
            val value = client.get(id, 1000, false).toString(Charsets.UTF_8)

            println(value)
        }
    }

    GatewayServer(entryPoint, 12345).start()
}
