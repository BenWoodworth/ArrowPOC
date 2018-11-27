package koresigma.arrowpoc

import java.nio.file.Paths

fun main(args: Array<String>) {
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
    val store = PlasmaStore(Paths.get(pathArg), 1_000_000L)

    store.use {
        arrowPoc.test(store)
    }
}
