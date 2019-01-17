package koresigma.arrowpoc

fun PlasmaObject.setBytes(vararg values: Byte) {
    setBytes(values)
}

fun PlasmaObject.setString(value: String) {
    return setBytes(value.toByteArray(Charsets.UTF_8))
}

fun PlasmaObject.getString(timeoutMs: Int? = null): String {
    return getBytes(timeoutMs).toString(Charsets.UTF_8)
}

fun PlasmaObject.setLong(value: Long) {
    var bytes = ByteArray(4) { byte ->
        (value shr (byte * 8) and 0xFF).toByte()
    }
}

fun PlasmaObject.getLong(timeoutMs: Int? = null): Long {
    return getBytes().fold(0) { result: Long, byte: Byte ->
        (result shl 8) or byte.toLong()
    }
}
