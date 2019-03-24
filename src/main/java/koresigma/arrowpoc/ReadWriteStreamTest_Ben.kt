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
        val fiveCsv = ReadWriteStreamTest_Ben::class.java.getResource("/data/million.csv")

        val writeStream = File(fiveCsv.file)
            .load1mVectors()
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

    private fun File.load100kVectors(): List<FieldVector> {
        val first = VarCharVector("first", allocator)
        val last = VarCharVector("last", allocator)
        val email = VarCharVector("email", allocator)
        val age = IntVector("age", allocator)
        val birthday = DateMilliVector("birthday", allocator)
        val ccnumber = BigIntVector("ccnumber", allocator)

        val vectors = listOf<FieldVector>(first, last, email, age, birthday, ccnumber)

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

    private fun File.load1mVectors(): List<FieldVector> {
        val age = SmallIntVector("age", allocator)
        val dollar = IntVector("dollar", allocator)
        val longitude = Float4Vector("longitude", allocator)
        val latitude = Float4Vector("latitude", allocator)
        val zip = IntVector("zip", allocator)
        val integer = IntVector("integer", allocator)
        val ccnumber = BigIntVector("ccnumber", allocator)

        val vectors = listOf<FieldVector>(age, dollar, longitude, latitude, zip, integer, ccnumber)

        useLines { csvLines ->
            val birthdayFormat = SimpleDateFormat("MM/DD/YYYY")

            fun String.dollarToInt() = 0 +
                    substring(1 until length - 3).toInt() * 100 +
                    substring(length - 2 until length).toInt()

            csvLines
                .drop(1) // Skip header
                .filter { !it.isBlank() }
                .map { it.split(",") }
                .forEachIndexed { i, entry ->
                    //TODO Is setSafe() slow?
                    age.setSafe(i, entry[0].toInt())
                    dollar.setSafe(i, entry[1].dollarToInt())
                    longitude.setSafe(i, entry[2].toFloat())
                    latitude.setSafe(i, entry[3].toFloat())
                    zip.setSafe(i, entry[4].toInt())
                    integer.setSafe(i, entry[5].toInt())
                    ccnumber.setSafe(i, entry[6].toLong())

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
