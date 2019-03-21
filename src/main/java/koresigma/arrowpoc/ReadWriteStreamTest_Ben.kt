package koresigma.arrowpoc

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.plasma.PlasmaClient
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.ArrowWriter
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.channels.Channels
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

    private fun readStream(out: ByteArray) {
        val `in` = ByteArrayInputStream(out)
        val reader = ArrowStreamReader(`in`, allocator)
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

    private fun writeStreamFromCsv(pathToFile: String): ByteArrayOutputStream {
        val os = ByteArrayOutputStream()
        val scanner = Scanner(File(pathToFile))
        scanner.nextLine()

        val vectors = ArrayList<FieldVector>()
        val fields = ArrayList<Field>()
        val vector = IntVector("csvtest", allocator)
        vectors.add(vector)
        fields.add(vector.field)

        val schema = Schema(fields, null)
        val root = VectorSchemaRoot(schema, vectors, 0)
        val writer = ArrowStreamWriter(root, null, Channels.newChannel(os))

        writer.start()
        val rows = HashMap<Int, MutableList<String>>()
        while (scanner.hasNextLine()) {
            val values = scanner.nextLine()
                .replace("$", "")
                .split(",".toRegex())
                .dropLastWhile { it.isEmpty() }

            for (i in values.indices) {
                if (rows.containsKey(i)) {
                    rows[i]!!.add(values[i])
                } else {
                    val newList = ArrayList<String>()
                    newList.add(values[i])
                    rows[i] = newList
                }
            }
        }

        for (values in rows.values) {
            writeBatch(writer, vector, values, root)
        }

        writer.end()
        return os
    }

    private fun writeBatch(writer: ArrowWriter, vector: IntVector, values: List<String>, root: VectorSchemaRoot) {
        for (i in values.indices) {
            var value = values[i]

            if (value.contains(".")) {
                value = value.substring(0, value.indexOf("."))
            }

            try {
                vector.setSafe(i, Integer.parseInt(value))
            } catch (nfe: NumberFormatException) {
                vector.setNull(i)
            }

        }

        vector.valueCount = values.size
        root.rowCount = values.size
        writer.writeBatch()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        //        System.loadLibrary("plasma_java");
        //        PlasmaClient plasmaClient = new PlasmaClient("/tmp/store", "", 0);
        val pathName = "src/main/resources/data/five.csv"
        readStream(writeStreamFromCsv(pathName).toByteArray())
        //        ByteArrayOutputStream out = writeStream();
        //        readStream(out.toByteArray());
        //        byte[] objectId = putValueInPlasma(plasmaClient, out.toByteArray());
        //        readStream(getValueFromPlasma(plasmaClient, objectId));

    }
}
