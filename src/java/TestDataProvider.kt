import models.CsvModel
import models.DummyCsvEntry
import models.DummyCsvEntryMillion
import java.io.File
import java.text.SimpleDateFormat

object TestDataProvider {

    fun getTestData() = sequence<TestData<*>> {
        val dateFormat = SimpleDateFormat("d/m/yyyy")


        yield(
            TestData(
                name = "hunThouModel.csv",
                serializer = CsvModel.serializer(DummyCsvEntry.serializer()),

                data = CsvModel.fromFile(
                    file = dataRes("hunThouModel.csv"),
                    hasHeader = true,
                    parseEntry = {
                        DummyCsvEntry(
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
                        name = "millionModel.csv",
                        serializer = CsvModel.serializer(DummyCsvEntryMillion.serializer()),

                        data = CsvModel.fromFile(
                                file = dataRes("millionModel.csv"),
                                hasHeader = true,
                                parseEntry = {
                                    DummyCsvEntryMillion(
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

    private fun dataRes(path: String): File {
        return File(Main::class.java.getResource("data/$path").file)
    }
}