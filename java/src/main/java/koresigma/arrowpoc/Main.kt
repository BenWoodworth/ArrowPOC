package koresigma.arrowpoc

import org.apache.arrow.plasma.PlasmaClientTest
import java.nio.file.Paths

fun main(args: Array<String>) {
    PlasmaClientTest().doTest()

    System.loadLibrary("plasma_java")

    val pathArg: String

    when (args.size) {
        0 ->
            pathArg = "/tmp/plasma"

        1 ->
            pathArg = args[0]

        else ->
            throw IllegalArgumentException("Takes one option argument: plasma store path")
    }

    val arrowPoc = ArrowPoc()
//    val store = PlasmaStore(Paths.get(pathArg), 1_000_000L)

//    store.use {
//        arrowPoc.test(store)
//    }

    arrowPoc.testWithExistingSocket(Paths.get("$pathArg"))
}
