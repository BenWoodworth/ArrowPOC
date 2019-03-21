package koresigma.arrowpoc

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.plasma.PlasmaClient
import org.apache.arrow.vector.*
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.Text
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.nio.channels.Channels
import java.text.SimpleDateFormat
import java.util.*

object ReadWriteStreamTest_Ben {
    private val allocator = RootAllocator(Integer.MAX_VALUE.toLong())

    private fun putValueInPlasma(plasmaClient: PlasmaClient, value: ByteArray): ByteArray {
        val objectId = ByteArray(20)
        Random().nextBytes(objectId)
        val metaData: ByteArray? = null
        plasmaClient.put(objectId, value, metaData)
        return objectId
    }

    private fun getValueFromPlasma(plasmaClient: PlasmaClient, objectId: ByteArray): ByteArray {
        val timeoutMs = 0
        val isMetadata = false
        return plasmaClient.get(objectId, timeoutMs, isMetadata)
    }

    private fun readStream(stream: InputStream) {
        val reader = ArrowStreamReader(stream, allocator)
        val vector = reader.vectorSchemaRoot.fieldVectors[0] as IntVector

        while (reader.loadNextBatch()) {
            for (i in 0 until vector.valueCount) {
                if (vector.isNull(i)) {
                    print("null ")
                } else {
                    print(vector.get(i).toString() + " ")
                }
            }
            println()
        }
    }

    private fun writeStreamFromCsv(csvFile: File): ByteArrayOutputStream {
        val output = ByteArrayOutputStream()

        val vectors = object : Iterable<FieldVector> {
            val first = VarCharVector("first", allocator)
            val last = VarCharVector("last", allocator)
            val email = VarCharVector("email", allocator)
            val age = IntVector("age", allocator)
            val birthday = DateMilliVector("birthday", allocator)
            val ccnumber = BigIntVector("ccnumber", allocator)

            override fun iterator(): Iterator<FieldVector> {
                return listOf<FieldVector>(first, last, email, age, birthday, ccnumber).iterator()
            }
        }

        val schema = Schema(vectors.toList().map { it.field })

        val root = VectorSchemaRoot(schema, vectors.toList(), 0)
        val writer = ArrowStreamWriter(root, null, Channels.newChannel(output))

        writer.start()
        csvFile.useLines { csvLines ->
            val birthdayFormat = SimpleDateFormat("MM/DD/YYYY")

            csvLines
                .drop(1) // Skip header
                .filter { !it.isBlank() }
                .map { it.split(",") }
                .forEachIndexed { i, entry ->
                    //TODO Is setSafe() slow?
                    vectors.first.setSafe(i, Text(entry[0]))
                    vectors.last.setSafe(i, Text(entry[1]))
                    vectors.email.setSafe(i, Text(entry[2]))
                    vectors.age.setSafe(i, entry[3].toInt())
                    vectors.birthday.setSafe(i, birthdayFormat.parse(entry[4]).time)
                    vectors.ccnumber.setSafe(i, entry[5].toLong())
                }
        }

        return output
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val fiveCsv = ReadWriteStreamTest_Ben::class.java.getResource("/data/five.csv")

        val writeStream = writeStreamFromCsv(File(fiveCsv.file))
        val byteArrayStream = ByteArrayInputStream(writeStream.toByteArray())
        readStream(byteArrayStream)
    }
}
