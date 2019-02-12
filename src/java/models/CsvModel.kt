package models

import java.io.File

class CsvModel(
    val rows: List<List<String>>
) {

    companion object {
        fun fromFile(file: File, separator: String = ","): CsvModel {
            return file.readLines()
                .map { it.split(separator) }
                .let { CsvModel(it) }
        }
    }
}