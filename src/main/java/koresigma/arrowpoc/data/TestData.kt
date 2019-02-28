package koresigma.arrowpoc.data

import kotlinx.serialization.KSerializer

interface TestData<T> {

    val name: String

    val serializer: KSerializer<T>

    fun getData(): T
}