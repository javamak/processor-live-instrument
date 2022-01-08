package spp.processor.live.impl.instrument

import com.google.common.io.Resources
import io.vertx.core.json.JsonObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import spp.protocol.instrument.LiveVariable

class LiveInstrumentAnalysisTest {

    @Test
    fun processBpHit1() {
        val bpData = JsonObject(
            Resources.toString(Resources.getResource("bphit1.json"), Charsets.UTF_8)
        )
        val bpHit = LiveInstrumentAnalysis.transformRawBreakpointHit(bpData)
        assertNotNull(bpHit)

        val topStack = bpHit.stackTrace.first()
        assertEquals(2, topStack.variables.size)

        val thisVar = topStack.variables[0]
        assertEquals("this", thisVar.name)
        assertEquals(3, (thisVar.value as List<*>).size)

        val s2Var = topStack.variables[1]
        assertEquals("s2", s2Var.name)
        assertEquals(4, (s2Var.value as List<*>).size)
        val s2Vars = s2Var.value as List<LiveVariable>
        val course = s2Vars.find { it.name == "course" }!!
        assertEquals(2, (course.value as List<LiveVariable>).size)
        assertEquals(
            2,
            ((course.value as List<LiveVariable>)
                .find { it.name == "time" }!!.value as List<LiveVariable>).size
        )

        val student2 = s2Vars.find { it.name == "student2" }!!
        assertEquals(3, (student2.value as List<LiveVariable>).size)
        assertEquals(
            2,
            ((student2.value as List<LiveVariable>)
                .find { it.name == "course" }!!.value as List<LiveVariable>).size
        )
        assertEquals(
            2,
            (((student2.value as List<LiveVariable>)
                .find { it.name == "course" }!!.value as List<LiveVariable>)
                .find { it.name == "time" }!!.value as List<LiveVariable>).size
        )
    }
}
