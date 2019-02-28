import org.apache.arrow.plasma.PlasmaClient
import java.io.File

class PlasmaStore(
    private val location: File,
    bytes: Int
) : AutoCloseable {

    private val server = ProcessBuilder()
        .command(
            "plasma_store_server",
            "-s", location.toString(),
            "-m", bytes.toString()
        )
        .start()

    init {
        System.loadLibrary("plasma_java")
    }

    override fun close() {
        server.destroy()
    }


    fun createClient(): PlasmaClient {
        return PlasmaClient(location.toString(), "", 0)
    }
}
