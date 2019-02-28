package koresigma.arrowpoc.data

import PerformanceTester
import PlasmaStore
import TestResult
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
                testData = listOf(
                    TestDataHundredThousand,
                    TestDataMillion
                ),
                serializeServices = listOf(
                    SerializeJson(),
                    SerializeCbor()
//                    SerializeProtoBuf()
//                   SerializeParquet()
                ),
                readWriteServices = listOf(
                    ReadWriteFile(testFile),
                    ReadWritePlasma(store, plasmaObject)
//                   ReadWriteVariable()
                )
            )
        }
    }

    private fun test(
        testData: List<TestData<*>>,
        serializeServices: List<Serialize>,
        readWriteServices: List<ReadWrite>
    ) {
        val tester = PerformanceTester(serializeServices, readWriteServices)

        testData.forEach { data ->
            println()

            println("Warming up '${data.name}'...")
            tester.test(data).forEach { }

            println("Testing '${data.name}'...")

            val results = tester.test(data)
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
