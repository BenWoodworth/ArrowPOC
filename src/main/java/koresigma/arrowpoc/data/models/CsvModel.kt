package koresigma.arrowpoc.data.models

import kotlinx.serialization.Serializable
import java.io.File

@Serializable
class CsvModel<T>(val entries: List<T>) {

    companion object {
        fun <T> fromFile(
            file: File,
            hasHeader: Boolean,
            separator: String = ",",
            parseEntry: (List<String>) -> T
        ): CsvModel<T> {
            file.useLines { lines ->
                return lines
                    .drop(if (hasHeader) 1 else 0)
                    .map { parseEntry(it.split(separator)) }
                    .toList()
                    .let { CsvModel(it) }
            }
        }
    }
}