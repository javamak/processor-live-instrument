package integration

import io.vertx.core.Promise
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.BridgeEventType
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameHelper
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import io.vertx.serviceproxy.ServiceProxyBuilder
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import spp.protocol.SourceMarkerServices
import spp.protocol.SourceMarkerServices.Provide
import spp.protocol.SourceMarkerServices.Status
import spp.protocol.instrument.LiveInstrumentEvent
import spp.protocol.instrument.LiveInstrumentEventType
import spp.protocol.instrument.LiveSourceLocation
import spp.protocol.instrument.breakpoint.LiveBreakpoint
import spp.protocol.instrument.breakpoint.event.LiveBreakpointHit
import spp.protocol.service.live.LiveInstrumentService
import spp.protocol.status.MarkerConnection
import java.util.*
import java.util.concurrent.TimeUnit

@ExtendWith(VertxExtension::class)
class InstrumentIntegrationTest : ProcessorIntegrationTest() {

    companion object {
        private val log = LoggerFactory.getLogger(InstrumentIntegrationTest::class.java)
    }

    @Test
    fun verifyLiveVariables() {
        val testContext = VertxTestContext()
        var gotAdded = false
        var gotHit = false
        var gotRemoved = false
        val instrumentId = UUID.randomUUID().toString()
        val consumer = vertx.eventBus().localConsumer<JsonObject>("local." + Provide.LIVE_INSTRUMENT_SUBSCRIBER)

        runBlocking {
            //send marker connected status
            val replyAddress2 = UUID.randomUUID().toString()
            val pc = MarkerConnection(INSTANCE_ID, System.currentTimeMillis())
            val consumer2: MessageConsumer<Boolean> = vertx.eventBus().localConsumer("local.$replyAddress2")

            val promise = Promise.promise<Void>()
            consumer2.handler {
                promise.complete()
                consumer2.unregister()
            }

            FrameHelper.sendFrame(
                BridgeEventType.SEND.name.toLowerCase(), Status.MARKER_CONNECTED,
                replyAddress2, JsonObject(), true, JsonObject.mapFrom(pc), tcpSocket
            )
            withTimeout(5000) {
                promise.future().await()
            }

            vertx.eventBus().localConsumer<JsonObject>(SourceMarkerServices.Utilize.LIVE_INSTRUMENT) { resp ->
                val forwardAddress = resp.address()
                val forwardMessage = resp.body()
                val replyAddress = UUID.randomUUID().toString()

                val tempConsumer = vertx.eventBus().localConsumer<Any>("local.$replyAddress")
                tempConsumer.handler {
                    resp.reply(it.body())
                    tempConsumer.unregister()
                }

                val headers = JsonObject()
                resp.headers().entries().forEach { headers.put(it.key, it.value) }
                FrameHelper.sendFrame(
                    BridgeEventType.SEND.name.toLowerCase(), forwardAddress,
                    replyAddress, headers, true, forwardMessage, tcpSocket
                )
            }

            //register listener
            FrameHelper.sendFrame(
                BridgeEventType.REGISTER.name.toLowerCase(),
                Provide.LIVE_INSTRUMENT_SUBSCRIBER, JsonObject(), tcpSocket
            )

            consumer.handler {
                log.info("Got subscription event: {}", it.body())
                val liveEvent = Json.decodeValue(it.body().toString(), LiveInstrumentEvent::class.java)
                println(liveEvent)

                when (liveEvent.eventType) {
                    LiveInstrumentEventType.BREAKPOINT_ADDED -> {
                        log.info("Got added")
                        testContext.verify {
                            assertEquals(instrumentId, JsonObject(liveEvent.data).getString("id"))
                        }
                        gotAdded = true
                    }
                    LiveInstrumentEventType.BREAKPOINT_REMOVED -> {
                        log.info("Got removed")
                        testContext.verify {
                            assertEquals(instrumentId, JsonObject(liveEvent.data).getString("breakpointId"))
                        }
                        gotRemoved = true
                    }
                    LiveInstrumentEventType.BREAKPOINT_HIT -> {
                        log.info("Got hit")
                        testContext.verify {
                            assertEquals(instrumentId, JsonObject(liveEvent.data).getString("breakpointId"))
                        }
                        gotHit = true

                        val bpHit = Json.decodeValue(liveEvent.data, LiveBreakpointHit::class.java)
                        testContext.verify {
                            assertTrue(bpHit.stackTrace.elements.isNotEmpty())
                            val topFrame = bpHit.stackTrace.elements.first()
                            assertEquals(9, topFrame.variables.size)

                            //byte
                            assertEquals(-2, topFrame.variables.find { it.name == "b" }!!.value)
                            assertEquals(
                                "java.lang.Byte",
                                topFrame.variables.find { it.name == "b" }!!.liveClazz
                            )

                            //char
                            assertEquals("h", topFrame.variables.find { it.name == "c" }!!.value)
                            assertEquals(
                                "java.lang.Character",
                                topFrame.variables.find { it.name == "c" }!!.liveClazz
                            )

                            //string
                            assertEquals("hi", topFrame.variables.find { it.name == "s" }!!.value)
                            assertEquals(
                                "java.lang.String",
                                topFrame.variables.find { it.name == "s" }!!.liveClazz
                            )

                            //double
                            assertEquals(0.23, topFrame.variables.find { it.name == "d" }!!.value)
                            assertEquals(
                                "java.lang.Double",
                                topFrame.variables.find { it.name == "d" }!!.liveClazz
                            )

                            //bool
                            assertEquals(true, topFrame.variables.find { it.name == "bool" }!!.value)
                            assertEquals(
                                "java.lang.Boolean",
                                topFrame.variables.find { it.name == "bool" }!!.liveClazz
                            )

                            //long
                            assertEquals(Long.MAX_VALUE, topFrame.variables.find { it.name == "max" }!!.value)
                            assertEquals(
                                "java.lang.Long",
                                topFrame.variables.find { it.name == "max" }!!.liveClazz
                            )

                            //short
                            assertEquals(
                                Short.MIN_VALUE.toInt(),
                                topFrame.variables.find { it.name == "sh" }!!.value
                            )
                            assertEquals(
                                "java.lang.Short",
                                topFrame.variables.find { it.name == "sh" }!!.liveClazz
                            )

                            //float
                            assertEquals(1.0, topFrame.variables.find { it.name == "f" }!!.value)
                            assertEquals(
                                "java.lang.Float",
                                topFrame.variables.find { it.name == "f" }!!.liveClazz
                            )

                            //integer
                            assertEquals(1, topFrame.variables.find { it.name == "i" }!!.value)
                            assertEquals(
                                "java.lang.Integer",
                                topFrame.variables.find { it.name == "i" }!!.liveClazz
                            )
                        }
                    }
                    else -> testContext.failNow("Got event: " + it.body())
                }

                if (gotAdded && gotHit && gotRemoved) {
                    consumer.unregister {
                        if (it.succeeded()) {
                            testContext.completeNow()
                        } else {
                            testContext.failNow(it.cause())
                        }
                    }
                }
            }.completionHandler {
                if (it.failed()) {
                    testContext.failNow(it.cause())
                    return@completionHandler
                }

                val instrumentService = ServiceProxyBuilder(vertx)
                    .setToken(SYSTEM_JWT_TOKEN)
                    .setAddress(SourceMarkerServices.Utilize.LIVE_INSTRUMENT)
                    .build(LiveInstrumentService::class.java)
                instrumentService.addLiveInstrument(
                    LiveBreakpoint(
                        id = instrumentId,
                        location = LiveSourceLocation("E2EApp", 24)
                    )
                ) {
                    if (it.failed()) {
                        testContext.failNow(it.cause())
                    }
                }
            }
        }

        if (testContext.awaitCompletion(30, TimeUnit.SECONDS)) {
            if (testContext.failed()) {
                consumer.unregister()
                log.info("Got added: $gotAdded")
                log.info("Got hit: $gotHit")
                log.info("Got removed: $gotRemoved")
                throw testContext.causeOfFailure()
            }
        } else {
            consumer.unregister()
            log.info("Got added: $gotAdded")
            log.info("Got hit: $gotHit")
            log.info("Got removed: $gotRemoved")
            throw RuntimeException("Test timed out")
        }
    }
}
