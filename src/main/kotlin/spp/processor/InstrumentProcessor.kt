package spp.processor

import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.BridgeEventType
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameHelper
import kotlinx.coroutines.runBlocking
import org.apache.skywalking.oap.server.library.module.ModuleManager
import org.slf4j.LoggerFactory
import spp.processor.common.FeedbackProcessor
import spp.protocol.SourceMarkerServices
import spp.protocol.SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER
import spp.protocol.platform.PlatformAddress
import spp.protocol.processor.ProcessorAddress
import kotlin.system.exitProcess

object InstrumentProcessor : FeedbackProcessor() {

    private val log = LoggerFactory.getLogger(InstrumentProcessor::class.java)
    var module: ModuleManager? = null

    init {
        runBlocking {
            log.info("InstrumentProcessor initialized")

            connectToPlatform()
            republishEvents(vertx, LIVE_INSTRUMENT_SUBSCRIBER)
            republishEvents(vertx, ProcessorAddress.BREAKPOINT_HIT.address)
            republishEvents(vertx, ProcessorAddress.LOG_HIT.address)
        }
    }

    override fun onConnected() {
        //todo: this is hacky. ServiceBinder.register is supposed to do this
        //register services
        FrameHelper.sendFrame(
            BridgeEventType.REGISTER.name.lowercase(),
            SourceMarkerServices.Utilize.LIVE_INSTRUMENT,
            JsonObject(), tcpSocket
        )

        FrameHelper.sendFrame(
            BridgeEventType.REGISTER.name.lowercase(),
            PlatformAddress.LIVE_BREAKPOINT_APPLIED.address,
            JsonObject(), tcpSocket
        )
        FrameHelper.sendFrame(
            BridgeEventType.REGISTER.name.lowercase(),
            PlatformAddress.LIVE_BREAKPOINT_REMOVED.address,
            JsonObject(), tcpSocket
        )

        //deploy processor
        log.info("Deploying source processor")
        vertx.deployVerticle(InstrumentProcessorVerticle()) {
            if (it.succeeded()) {
                processorVerticleId = it.result()
            } else {
                log.error("Failed to deploy source processor", it.cause())
                exitProcess(-1)
            }
        }
    }
}
