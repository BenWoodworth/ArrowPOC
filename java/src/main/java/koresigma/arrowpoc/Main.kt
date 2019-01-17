package koresigma.arrowpoc

import java.nio.file.Paths

fun main(args: Array<String>) {
    System.loadLibrary("plasma_java")

    val pathArg = when (args.size) {
        0 -> "/tmp/plasma"
        1 -> args[0]
        else -> throw IllegalArgumentException("Takes one option argument: plasma store path")
    }

    val arrowPoc = ArrowPoc()
//    val store = PlasmaStore(Paths.get(pathArg), 1_000_000L)

//    store.use {
//        arrowPoc.test(store)
//    }

    arrowPoc.testWithExistingSocket(Paths.get("$pathArg"))
}
