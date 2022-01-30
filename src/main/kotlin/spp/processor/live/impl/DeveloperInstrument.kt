package spp.processor.live.impl

import spp.processor.common.DeveloperAuth
import spp.protocol.instrument.LiveInstrument
import java.util.*

data class DeveloperInstrument(
    val developerAuth: DeveloperAuth,
    val instrument: LiveInstrument
) {
    //todo: verify selfId isn't needed in equals/hashcode
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DeveloperInstrument) return false
        if (instrument != other.instrument) return false
        return true
    }

    override fun hashCode(): Int = Objects.hash(developerAuth.selfId, instrument)
}