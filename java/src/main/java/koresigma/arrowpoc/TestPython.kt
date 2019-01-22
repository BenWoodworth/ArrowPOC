@file:Suppress("EXPERIMENTAL_API_USAGE")

package koresigma.arrowpoc

import java.io.File

fun testPython(store: PlasmaStore) {
    val client = store.createClient()

    val id = ByteArray(20) { it.toByte() }
    val value = "F00DBEEF".getHexBytes()

    val pythonFile = File("arrow_test.py")

    println("Java:   Writing to store")
    client.put(id, value, null)
    println()

    println("Python: Reading from store")
    runPython(pythonFile, "read", id.toHexString())
    println()

    println("Python: Writing to store")
    runPython(pythonFile, "write", id.toHexString(), value.toHexString())
    println()

    println("Java:   Reading from store")
    println(client.get(id, 1000, false).toHexString())
    println()
}

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

fun runPython(file: File, vararg args: String): String {
    val process = ProcessBuilder()
        .command("python", file.absolutePath, *args)
        .redirectOutput(ProcessBuilder.Redirect.PIPE)
        .start()

    return process.inputStream
        .bufferedReader()
        .readLine()
}
