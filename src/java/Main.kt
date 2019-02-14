import test.*
import java.io.File

object Main {
    private val testFile: File = File("/tmp/ArrowPocTestFile")
    private val plasmaStoreFile: File = File("/tmp/plasma")
    private val plasmaObject: ByteArray = ByteArray(20)

    @JvmStatic
    fun main(vararg args: String) {
        PlasmaStore(plasmaStoreFile, 1000000000).use { store ->
            test(
                listOf(
                    SerializeJson(),
                    SerializeCbor(),
                    SerializeProtoBuf()
//                   SerializeParquet()
                ),
                listOf(
                    ReadWriteFile(testFile),
                    ReadWritePlasma(store, plasmaObject)
//                   ReadWriteVariable()
                )
            )
        }
    }

    private fun test(
        serializers: List<Serialize>,
        readWriters: List<ReadWrite>
    ) {
        val tester = PerformanceTester(serializers, readWriters)

        TestDataProvider.getTestData().forEach { testData ->
            println()

            println("Warming up '${testData.name}'...")
            tester.test(testData).forEach { }

            println("Testing '${testData.name}'...")

            val results = tester.test(testData)
            printResults(results)
        }

        println()
        println("Done!")
    }

    private fun printResults(results: Sequence<TestResult>) {
        print("Serial Format,")
        print("ReadWrite Format,")
        print("Serialize Time (ns),")
        print("Write Time (ns),")
        print("Read Time (ns),")
        print("Deserialize Time (ns),")
        print("Serialized Size (b)\n")

        results.forEach { result ->
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
