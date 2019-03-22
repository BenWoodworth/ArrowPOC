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

    @JvmStatic
    fun main(args: Array<String>) {
        val fiveCsv = ReadWriteStreamTest_Ben::class.java.getResource("/data/five.csv")

        val writeStream = File(fiveCsv.file)
            .loadVectors()
            .toArrowStream()

        val byteArrayStream = ByteArrayInputStream(writeStream.toByteArray())
        readStream(byteArrayStream)
    }

    private fun readStream(stream: InputStream) {
        val reader = ArrowStreamReader(stream, allocator)
        val root = reader.vectorSchemaRoot
        val vectors = root.fieldVectors

        while (reader.loadNextBatch()) {
            val rows = reader.vectorSchemaRoot.rowCount

            for (vector in vectors) {
                print("${vector.field.name} (${vector.field.type}):")

                for (i in 0 until rows) {
                    print("\t${vector.getObject(i)}")
                }

                println()
            }
        }
    }

    private fun File.loadVectors(): List<FieldVector> {
        val first = VarCharVector("first", allocator)
        val last = VarCharVector("last", allocator)
        val email = VarCharVector("email", allocator)
        val age = IntVector("age", allocator)
        val birthday = DateMilliVector("birthday", allocator)
        val ccnumber = BigIntVector("ccnumber", allocator)

        val vectors = listOf<FieldVector> (first, last, email, age, birthday, ccnumber)

        useLines { csvLines ->
            val birthdayFormat = SimpleDateFormat("MM/DD/YYYY")

            csvLines
                .drop(1) // Skip header
                .filter { !it.isBlank() }
                .map { it.split(",") }
                .forEachIndexed { i, entry ->
                    //TODO Is setSafe() slow?
                    first.setSafe(i, Text(entry[0]))
                    last.setSafe(i, Text(entry[1]))
                    email.setSafe(i, Text(entry[2]))
                    age.setSafe(i, entry[3].toInt())
                    birthday.setSafe(i, birthdayFormat.parse(entry[4]).time)
                    ccnumber.setSafe(i, entry[5].toLong())

                    vectors.forEach { it.valueCount = i + 1 }
                }
        }

        return vectors
    }

    private fun List<FieldVector>.toArrowStream(): ByteArrayOutputStream {
        val output = ByteArrayOutputStream()

        val schema = Schema(this.map { it.field })
        val root = VectorSchemaRoot(schema, this, this[0].valueCount)

        val writer = ArrowStreamWriter(root, null, Channels.newChannel(output))
        writer.writeBatch()

        return output
    }
}
