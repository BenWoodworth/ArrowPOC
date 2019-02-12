import test.*
import java.io.File

private const val PLATFORM_JVM = "jvm"
private const val PLATFORM_PY = "python"

private const val FORMAT_FILE = "file"
private const val FORMAT_PLASMA = "plasma"
private const val FORMAT_VARIABLE = "variable"
private const val FORMAT_CBOR = "cbor"
private const val FORMAT_JSON = "json"
private const val FORMAT_PARQUET = "parquet"
private const val FORMAT_PROTOBUF = "protobuf"

object Main {
    private val testFile: File = File("/tmp/ArrowPocTestFile")
    private val plasmaStoreFile: File = File("/tmp/plasma")
    private val plasmaObject: ByteArray = ByteArray(20)

    private fun test(
        serializers: List<ServiceInfo<Serialize>>,
        readWriters: List<ServiceInfo<ReadWrite>>
    ) {
        val tester = PerformanceTester(serializers, readWriters)

        TestDataProvider.getTestData().forEach { testData ->
            println()

            println("Warming up '${testData.name}'...")
            tester.test(testData)

            println("Testing '${testData.name}'...")

            val results = tester.test(testData)
            printResults(results)
        }

        println()
        println("Done!")
    }

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

        PlasmaStore(plasmaStoreFile, 1000000000).use { store ->
            test(
                getJvmSerializeServices(),
                getJvmReadWriteServices(testFile, store, plasmaObject)
            )
        }
    }

    private fun getJvmSerializeServices(): List<ServiceInfo<Serialize>> {
        return listOf(
            ServiceInfo(PLATFORM_JVM, FORMAT_JSON, SerializeJson()),
            ServiceInfo(PLATFORM_JVM, FORMAT_CBOR, SerializeCbor()),
            ServiceInfo(PLATFORM_JVM, FORMAT_PROTOBUF, SerializeProtoBuf())
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
            ServiceInfo(PLATFORM_JVM, FORMAT_PLASMA, ReadWritePlasma(plasmaStore, plasmaObject)),
            ServiceInfo(PLATFORM_JVM, FORMAT_VARIABLE, ReadWriteVariable())
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


    private fun printResults(results: Sequence<PerformanceTester.TestResult>) {
        print("From Platform,")
        print("To Platform,")
        print("Serial Format,")
        print("ReadWrite Format,")
        print("Serialize Time (ns),")
        print("Write Time (ns),")
        print("Read Time (ns),")
        print("Deserialize Time (ns),")
        print("Serialized Size (b)\n")

        results.forEach { result ->
            print("${result.fromPlatform},")
            print("${result.toPlatform},")
            print("${result.serialFormat},")
            print("${result.readWriteFormat},")
            print("${result.serializeDuration},")
            print("${result.writeDuration},")
            print("${result.readDuration},")
            print("${result.deserializeDuration},")
            print("${result.serializedSize}\n")
        }
    }
}
