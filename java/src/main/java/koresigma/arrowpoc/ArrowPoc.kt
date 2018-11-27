package koresigma.arrowpoc

class ArrowPoc {

    fun test(plasmaStore: PlasmaStore) {
        val client = plasmaStore.createClient()

        val obj = client.getObject("helloworld")

        println(
            """
                Exists: ${obj.exists}
                Value:  ${obj.getString()}
            """.trimIndent()
        )
    }
}
