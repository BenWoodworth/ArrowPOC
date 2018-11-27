package koresigma.arrowpoc

import org.apache.arrow.plasma.PlasmaClient
import java.io.IOException
import java.nio.file.Path

class PlasmaStore(
    private val path: Path,
    byteSize: Long
) : AutoCloseable {

    private val process: Process

    init {
        try {
            process = ProcessBuilder()
                .command(
                    "plasma_store",
                    "-s", path.toString(),
                    "-m", java.lang.Long.toString(byteSize)
                )
                .start()
        } catch (exception: IOException) {
            throw IOException("Unable to create plasma store", exception)
        }
    }

    fun createClient(): PlasmaClient {
        System.loadLibrary("plasma_java")
        return PlasmaClient(path.toString(), "", 0)
    }

    override fun close() {
        process.destroy()
    }
}
