package koresigma.arrowpoc

import org.apache.arrow.plasma.PlasmaClient
import java.nio.file.Path

class ArrowPoc {

    fun test(plasmaStore: PlasmaStore) {
        test(plasmaStore.createClient())
    }

    fun testWithExistingSocket(storeSocket: Path) {
        test(PlasmaClient(storeSocket.toString(), "", 0))
    }

    private fun test(client: PlasmaClient) {
        val id = ByteArray(20) { 1 }

        val obj = client.getObject(id)

        val str = "Hello, world!"
        println("Putting '$str'")
        obj.setString(str)

        println(
            """
                Checking for put string...

                ========================================
                Exists: ${obj.exists}
                Value:  ${obj.getString()}
                ========================================
            """.trimIndent()
        )
    }
}
