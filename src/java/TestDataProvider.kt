import models.CsvModel
import models.MillionCsvEntry
import models.HundredThousandCsvEntry
import java.io.File
import java.text.SimpleDateFormat

object TestDataProvider {

    fun getTestData() = sequence<TestData<*>> {
        val dateFormat = SimpleDateFormat("d/m/yyyy")


        yield(
            TestData(
                name = "hundred-thousand-actual.csv",
                serializer = CsvModel.serializer(MillionCsvEntry.serializer()),

                data = CsvModel.fromFile(
                    file = getDataResource("hundred-thousand-actual.csv"),
                    hasHeader = true,
                    parseEntry = {
                        MillionCsvEntry(
                            first = it[0],
                            last = it[1],
                            email = it[2],
                            age = it[3].toInt(),
                            birthday = dateFormat.parse(it[4]).time,
                            ccnumber = it[5].toLong()
                        )
                    }
                )
            )
        )

        yield(
                TestData(
                        name = "million-actual.csv",
                        serializer = CsvModel.serializer(HundredThousandCsvEntry.serializer()),

                        data = CsvModel.fromFile(
                                file = getDataResource("million-actual.csv"),
                                hasHeader = true,
                                parseEntry = {
                                    HundredThousandCsvEntry(
                                            age = it[0].toInt(),
                                            dollar = it[1],
                                            longitude = it[2].toDouble(),
                                            latitude = it[3].toDouble(),
                                            zip = it[4].toInt(),
                                            integer = it[5].toInt(),
                                            ccnumber = it[6].toLong()
                                    )
                                }
                        )
                )
        )
    }

    private fun getDataResource(path: String): File {
        return File(Main::class.java.getResource("data/$path").file)
    }
}