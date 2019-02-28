package koresigma.arrowpoc.data

import koresigma.arrowpoc.Main
import java.io.File

abstract class TestDataCsv<TRow> : TestData<List<TRow>> {

    protected abstract val file: File

    protected abstract fun parseEntry(row: List<String>): TRow

    override fun getData(): List<TRow> {
        file.useLines { lines ->
            return lines
                .drop(1)
                .map { parseEntry(it.split(",")) }
                .toList()
        }
    }

    protected fun resource(path: String): File {
        return File(Main::class.java.getResource(path).file)
    }
}