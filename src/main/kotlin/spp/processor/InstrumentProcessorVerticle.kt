package spp.processor

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.servicediscovery.Record
import io.vertx.servicediscovery.types.EventBusService
import io.vertx.serviceproxy.ServiceBinder
import kotlinx.datetime.Instant
import org.apache.skywalking.oap.server.core.analysis.metrics.DataTable
import org.slf4j.LoggerFactory
import spp.processor.common.FeedbackProcessor
import spp.processor.common.FeedbackProcessor.Companion.INSTANCE_ID
import spp.processor.live.LiveInstrumentProcessor
import spp.processor.live.impl.LiveInstrumentProcessorImpl
import spp.protocol.processor.ProcessorAddress
import spp.protocol.util.KSerializers
import kotlin.system.exitProcess

class InstrumentProcessorVerticle : CoroutineVerticle() {

    companion object {
        private val log = LoggerFactory.getLogger(InstrumentProcessorVerticle::class.java)

        val liveInstrumentProcessor = LiveInstrumentProcessorImpl()
    }

    private var liveInstrumentRecord: Record? = null

    override suspend fun start() {
        log.info("Starting InstrumentProcessorVerticle")
        val module = SimpleModule()
        module.addSerializer(DataTable::class.java, object : JsonSerializer<DataTable>() {
            override fun serialize(value: DataTable, gen: JsonGenerator, provider: SerializerProvider) {
                val data = mutableMapOf<String, Long>()
                value.keys().forEach { data[it] = value.get(it) }
                gen.writeStartObject()
                data.forEach {
                    gen.writeNumberField(it.key, it.value)
                }
                gen.writeEndObject()
            }
        })
        DatabindCodec.mapper().registerModule(module)

        module.addSerializer(Instant::class.java, KSerializers.KotlinInstantSerializer())
        module.addDeserializer(Instant::class.java, KSerializers.KotlinInstantDeserializer())
        DatabindCodec.mapper().registerModule(module)

        vertx.deployVerticle(liveInstrumentProcessor).await()

        ServiceBinder(vertx).setIncludeDebugInfo(true)
            .setAddress(ProcessorAddress.LIVE_INSTRUMENT_PROCESSOR.address)
            .register(LiveInstrumentProcessor::class.java, liveInstrumentProcessor)
        liveInstrumentRecord = EventBusService.createRecord(
            ProcessorAddress.LIVE_INSTRUMENT_PROCESSOR.address,
            ProcessorAddress.LIVE_INSTRUMENT_PROCESSOR.address,
            LiveInstrumentProcessor::class.java,
            JsonObject().put("INSTANCE_ID", INSTANCE_ID)
        )
        FeedbackProcessor.discovery.publish(liveInstrumentRecord) {
            if (it.succeeded()) {
                log.info("Live instrument processor published")
            } else {
                log.error("Failed to publish live instrument processor", it.cause())
                exitProcess(-1)
            }
        }
    }

    override suspend fun stop() {
        log.info("Stopping InstrumentProcessorVerticle")
        FeedbackProcessor.discovery.unpublish(liveInstrumentRecord!!.registration).onComplete {
            if (it.succeeded()) {
                log.info("Live instrument processor unpublished")
            } else {
                log.error("Failed to unpublish live instrument processor", it.cause())
            }
        }.await()
    }
}
