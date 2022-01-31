/*
 * Source++, the open-source live coding platform.
 * Copyright (C) 2022 CodeBrig, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package spp.processor.live.impl.instrument

import io.vertx.core.Vertx
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import spp.processor.live.impl.LiveInstrumentProcessorImpl
import spp.protocol.instrument.LiveSourceLocation
import spp.protocol.instrument.breakpoint.LiveBreakpoint
import spp.protocol.probe.ProbeAddress

@ExtendWith(VertxExtension::class)
class LiveInstrumentControllerTest {

//    @Test
//    fun expiredPendingInstrument(vertx: Vertx, testContext: VertxTestContext) {
//        vertx.eventBus().consumer<Any>(ProbeAddress.LIVE_BREAKPOINT_REMOTE.address + ":" + "probeId") {
//            //ignore
//        }
//
//        val instrumentController = LiveInstrumentProcessorImpl()
//        instrumentController.addBreakpoint(
//            "system",
//            LiveBreakpoint(
//                LiveSourceLocation("test.LiveInstrumentControllerTest", 1),
//                expiresAt = System.currentTimeMillis(),
//                pending = true
//            )
//        )
//
//        GlobalScope.launch {
//            delay(2000) //wait for bp to be expired
//
//            testContext.verify {
//                assertEquals(0, instrumentController.getLiveInstruments().size)
//                testContext.completeNow()
//            }
//        }
//    }
}
