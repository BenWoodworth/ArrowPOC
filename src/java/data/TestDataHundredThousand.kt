package data

import kotlinx.serialization.Serializable
import kotlinx.serialization.list
import java.text.SimpleDateFormat

object TestDataHundredThousand : TestDataCsv<TestDataHundredThousand.Entry>() {

    override val name = "hundred-thousand"
    override val file = resource("data/hundred-thousand.csv")
    override val serializer = Entry.serializer().list

    private val dateFormat = SimpleDateFormat("d/m/yyyy")

    override fun parseEntry(row: List<String>): Entry {
        return Entry(
            first = row[0],
            last = row[1],
            email = row[2],
            age = row[3].toInt(),
            birthday = dateFormat.parse(row[4]).time,
            ccnumber = row[5].toLong()
        )
    }

    @Serializable
    class Entry(
        val first: String,
        val last: String,
        val email: String,
        val age: Int,
        val birthday: Long,
        val ccnumber: Long
    )
}