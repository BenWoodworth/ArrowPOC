package koresigma.arrowpoc

fun PlasmaObject.putBytes(vararg values: Byte) {
    setBytes(values)
}

fun PlasmaObject.putString(value: String) {
    return setBytes(value.toByteArray())
}

fun PlasmaObject.getString(timeoutMs: Int? = null): String {
    return getBytes(timeoutMs).toString()
}

fun PlasmaObject.putLong(value: Long) {
    var bytes = ByteArray(4) { byte ->
        (value shr (byte * 8) and 0xFF).toByte()
    }
}

fun PlasmaObject.getLong(timeoutMs: Int? = null): Long {
    return getBytes().fold(0) { result: Long, byte: Byte ->
        (result shl 8) or byte.toLong()
    }
}
