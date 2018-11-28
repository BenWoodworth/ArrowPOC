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
        val value = ByteArray(20) { 1 }

        val obj = client.getObject(id)

        println(client.contains(id))

        client.put(id, value, null)

//        client.wait(arrayOf(id), 1000, 1)
//        client.evict(200)
//        obj.fetch()
//        obj.setBytes(value)
        println(client.contains(id))
//        obj.release()

        println(
            """
                ========================================
                Exists: ${obj.exists}
                Value:  ${obj.getString()}
                ========================================
            """.trimIndent()
        )

        println("XXX")
    }
}
