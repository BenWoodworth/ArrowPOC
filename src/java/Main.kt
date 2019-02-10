import py4j.GatewayServer
import test.*
import java.io.File

private const val PLATFORM_JVM = "jvm"
private const val PLATFORM_PY = "python"

private const val FORMAT_FILE = "file"
private const val FORMAT_PLASMA = "plasma"
private const val FORMAT_JSON = "json"
private const val FORMAT_PARQUET = "parquet"

object Main {

    @JvmStatic
    fun main(args: Array<String>) {
        GatewayServer(Main, 12345).start()
    }

    fun pythonEntry(pythonServiceFactory: PythonServiceFactory) {
        val testFile = File("/tmp/ArrowPocTestFile")
        val testObjId = ByteArray(20)

        with(pythonServiceFactory) {
            PlasmaStore(File("/tmp/plasma"), 1000000).use { plasmaStore ->
                test(
                    listOf(
                        ServiceInfo(PLATFORM_JVM, FORMAT_JSON, SerializeJson()),
                        ServiceInfo(PLATFORM_JVM, FORMAT_PARQUET, SerializeJson()),
                        ServiceInfo(PLATFORM_PY, FORMAT_JSON, SerializeJsonPy()),
                        ServiceInfo(PLATFORM_PY, FORMAT_PARQUET, SerializeParquetPy())
                    ),
                    listOf(
                        ServiceInfo(PLATFORM_JVM, FORMAT_FILE, ReadWriteFile(testFile)),
                        ServiceInfo(PLATFORM_JVM, FORMAT_PLASMA, ReadWritePlasma(plasmaStore, testObjId)),
                        ServiceInfo(PLATFORM_PY, FORMAT_FILE, ReadWriteFilePy(testFile.path)),
                        ServiceInfo(PLATFORM_PY, FORMAT_PLASMA, ReadWritePlasmaPy(plasmaStore.location.path, testObjId))
                    )
                )
            }
        }
    }

    private fun test(
        serializers: List<ServiceInfo<Serialize>>,
        readWriters: List<ServiceInfo<ReadWrite>>
    ) {
        val tester = PerformanceTester(serializers, readWriters)

        val results = tester.test("Hello, world!")
        results.forEach {
            println(it)
        }
    }
}

