package koresigma.arrowpoc

import org.apache.arrow.plasma.PlasmaClient

fun PlasmaClient.getObject(id: ByteArray): PlasmaObject {
    return PlasmaObject(this, id)
}

fun PlasmaClient.getObject(id: String): PlasmaObject {
    return getObject(id.toByteArray())
}
