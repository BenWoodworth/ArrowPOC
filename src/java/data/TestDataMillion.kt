package data

import kotlinx.serialization.Serializable
import kotlinx.serialization.list

object TestDataMillion : TestDataCsv<TestDataMillion.Entry>() {

    override val name = "million"
    override val file = resource("data/million.csv")
    override val serializer = Entry.serializer().list

    override fun parseEntry(row: List<String>): Entry {
        return Entry(
            age = row[0].toInt(),
            dollar = row[1],
            longitude = row[2].toDouble(),
            latitude = row[3].toDouble(),
            zip = row[4].toInt(),
            integer = row[5].toInt(),
            ccnumber = row[6].toLong()
        )
    }

    @Serializable
    class Entry(
        val age: Int,
        val dollar: String,
        val longitude: Double,
        val latitude: Double,
        val zip: Int,
        val integer: Int,
        val ccnumber: Long
    )
}