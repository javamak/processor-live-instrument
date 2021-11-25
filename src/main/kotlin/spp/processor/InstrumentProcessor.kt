package spp.processor

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.BridgeEventType
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameHelper
import io.vertx.servicediscovery.ServiceDiscovery
import io.vertx.servicediscovery.ServiceDiscoveryOptions
import kotlinx.coroutines.runBlocking
import org.apache.skywalking.oap.server.library.module.ModuleManager
import org.slf4j.LoggerFactory
import spp.processor.common.FeedbackProcessor
import spp.protocol.processor.ProcessorAddress
import kotlin.system.exitProcess

object InstrumentProcessor : FeedbackProcessor() {

    private val log = LoggerFactory.getLogger(InstrumentProcessor::class.java)
    var module: ModuleManager? = null

    init {
        runBlocking {
            log.info("InstrumentProcessor initialized")
            vertx = Vertx.vertx()

            republishEvents(vertx, ServiceDiscoveryOptions.DEFAULT_ANNOUNCE_ADDRESS)
            republishEvents(vertx, ServiceDiscoveryOptions.DEFAULT_USAGE_ADDRESS)
            republishEvents(vertx, ProcessorAddress.VIEW_SUBSCRIPTION_EVENT.address)
            republishEvents(vertx, ProcessorAddress.BREAKPOINT_HIT.address)
            republishEvents(vertx, ProcessorAddress.LOG_HIT.address)

            discovery = ServiceDiscovery.create(vertx)
            connectToPlatform {
                //todo: something with the bool?

                //todo: this is hacky. ServiceBinder.register is supposed to do this
                //register services
                FrameHelper.sendFrame(
                    BridgeEventType.REGISTER.name.lowercase(),
                    ProcessorAddress.LIVE_VIEW_PROCESSOR.address,
                    JsonObject(), tcpSocket
                )
                FrameHelper.sendFrame(
                    BridgeEventType.REGISTER.name.lowercase(),
                    ProcessorAddress.LIVE_INSTRUMENT_PROCESSOR.address,
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
    }
}
