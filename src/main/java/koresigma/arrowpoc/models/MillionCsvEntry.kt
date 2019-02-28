package koresigma.arrowpoc.models

import kotlinx.serialization.Serializable

@Serializable
class MillionCsvEntry(
    val first: String,
    val last: String,
    val email: String,
    val age: Int,
    val birthday: Long,
    val ccnumber: Long
)