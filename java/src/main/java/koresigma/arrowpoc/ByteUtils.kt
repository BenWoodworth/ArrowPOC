package koresigma.arrowpoc

fun ByteArray.toHexString(): String {
    return joinToString("") {
        it.toString(16).toUpperCase()
    }
}

fun String.getHexBytes(): ByteArray {
    return ByteArray(length / 2) { i ->
        "${this[i * 2]}${this[i * 2 + 1]}".toByte(16)
    }
}
