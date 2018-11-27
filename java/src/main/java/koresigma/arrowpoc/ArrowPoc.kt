package koresigma.arrowpoc

class ArrowPoc {

    fun test(plasmaStore: PlasmaStore) {
        val client = plasmaStore.createClient()

        val id = ByteArray(20) { it.toByte() }

        val obj = client.getObject(id)

        obj.setString("Hello, world!")

        println(
            """
                ========================================
                Exists: ${obj.exists}
                Value:  ${obj.getString()}
                ========================================
            """.trimIndent()
        )
    }
}
