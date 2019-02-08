import org.apache.arrow.plasma.PlasmaClient
import java.nio.file.Path

class PlasmaStore(
    val path: Path,
    bytes: Int
) : AutoCloseable {

    private val server = ProcessBuilder()
        .command(
            "plasma_store_server",
            "-s", path.toString(),
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
        return PlasmaClient(path.toString(), "", 0)
    }
}
