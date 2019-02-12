package models

import kotlinx.serialization.Serializable
import java.io.File

@Serializable
class CsvModel(
    val header: List<String>,
    val data: List<List<*>>
) {

    companion object {
        fun fromFile(file: File, hasHeader: Boolean, separator: String = ",", columns: List<Column<*>>): CsvModel {
            return CsvModel(
                columns
                    .map { it.name },
                file.readLines()
                    .drop(if (hasHeader) 1 else 0)
                    .map { rowString ->
                        rowString
                            .split(separator)
                            .mapIndexed { i, s -> columns[i].parse(s) }
                    }
            )
        }
    }

    class Column<out T>(
        val name: String,
        val parse: (value: String) -> T
    )
}