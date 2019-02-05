package koresigma.arrowpoc

@ExperimentalUnsignedTypes
fun ByteArray.toHexString(): String {
    return joinToString("") {
        it.toUByte().toString(16).toUpperCase()
    }
}

fun String.getHexBytes(): ByteArray {
    return ByteArray(length / 2) { i ->
        "${this[i * 2]}${this[i * 2 + 1]}".toInt(16).toByte()
    }
}
