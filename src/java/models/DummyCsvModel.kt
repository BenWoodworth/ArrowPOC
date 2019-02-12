package models

import kotlinx.serialization.Serializable
import java.io.File
import java.text.SimpleDateFormat

@Serializable
class DummyCsvModel(val rows: List<Row>) {

    companion object {
        private val dateFormat = SimpleDateFormat("m/d/yyyy")

        fun fromFile(file: File): DummyCsvModel {
            return file.readLines()
                .drop(1)
                .map { it.split(",") }
                .map {
                    Row(
                        it[0],
                        it[1],
                        it[2],
                        it[3].toInt(),
                        dateFormat.parse(it[4]).time,
                        it[5].toLong()
                    )
                }
                .let { DummyCsvModel(it) }
        }
    }

    @Serializable
    class Row(
        val first: String,
        val last: String,
        val email: String,
        val age: Int,
        val birthday: Long,
        val ccnumber: Long
    )
}