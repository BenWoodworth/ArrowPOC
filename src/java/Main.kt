import test.*
import java.io.File

private const val PLATFORM_JVM = "jvm"
private const val PLATFORM_PY = "python"

private const val FORMAT_FILE = "file"
private const val FORMAT_PLASMA = "plasma"
private const val FORMAT_JSON = "json"
private const val FORMAT_PARQUET = "parquet"

object Main {

    private val testFile: File = File("/tmp/ArrowPocTestFile")
    private val plasmaStoreFile: File = File("/tmp/plasma")
    private val plasmaObject: ByteArray = ByteArray(20)

    @JvmStatic
    fun main(args: Array<String>) {
//        println("!!!Main")
//
//        val python = ProcessBuilder()
//            .command(
//                "python3",
//                javaClass.getResource("python/__main__.py").path
//            )
//            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
//            .redirectInput(ProcessBuilder.Redirect.INHERIT)
//            .redirectError(ProcessBuilder.Redirect.INHERIT)
//            .start()
//
//        val gatewayServer = GatewayServer(Main)
//        gatewayServer.start()
//        println("!!!Gateway opened")
//
////        python.destroy()
////        gatewayServer.shutdown()
//        println("!!!Done")

        PlasmaStore(plasmaStoreFile, 1000000).use { store ->
            test(
                getJvmSerializeServices(),
                getJvmReadWriteServices(testFile, store, plasmaObject)
            )
        }
    }

    private fun getJvmSerializeServices(): List<ServiceInfo<Serialize>> {
        return listOf(
            ServiceInfo(PLATFORM_JVM, FORMAT_JSON, SerializeJson())
//            ServiceInfo(PLATFORM_JVM, FORMAT_PARQUET, SerializeParquet())
        )
    }

    private fun getJvmReadWriteServices(
        testFile: File,
        plasmaStore: PlasmaStore,
        plasmaObject: ByteArray
    ): List<ServiceInfo<ReadWrite>> {
        return listOf(
            ServiceInfo(PLATFORM_JVM, FORMAT_FILE, ReadWriteFile(testFile)),
            ServiceInfo(PLATFORM_JVM, FORMAT_PLASMA, ReadWritePlasma(plasmaStore, plasmaObject))
        )
    }


    fun pythonEntry(pythonServiceFactory: PythonServiceFactory) {
        println("!!!Python entry")

        with(pythonServiceFactory) {
            PlasmaStore(plasmaStoreFile, 1000000).use { plasmaStore ->
                test(
                    listOf(
                        ServiceInfo(PLATFORM_PY, FORMAT_JSON, SerializeJsonPy()),
                        ServiceInfo(PLATFORM_PY, FORMAT_PARQUET, SerializeParquetPy())
                    ).plus(
                        getJvmSerializeServices()
                    ),
                    listOf(
                        ServiceInfo(PLATFORM_PY, FORMAT_FILE, ReadWriteFilePy(testFile.path)),
                        ServiceInfo(
                            PLATFORM_PY,
                            FORMAT_PLASMA,
                            ReadWritePlasmaPy(plasmaStore.location.path, plasmaObject)
                        )
                    ).plus(
                        getJvmReadWriteServices(testFile, plasmaStore, plasmaObject)
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

        print("From Platform\t")
        print("To Platform\t")
        print("Serial Format\t")
        print("ReadWrite Format\t")
        print("Serialize Duration\t")
        print("Write Duration\t")
        print("Read Duration\t")
        print("Deserialize Duration\n")

        results.forEach { result ->
            print("${result.fromPlatform}\t")
            print("${result.toPlatform}\t")
            print("${result.serialFormat}\t")
            print("${result.readWriteFormat}\t")
            print("${result.serializeDuration}\t")
            print("${result.writeDuration}\t")
            print("${result.readDuration}\t")
            print("${result.deserializeDuration}\n")
        }
    }
}
