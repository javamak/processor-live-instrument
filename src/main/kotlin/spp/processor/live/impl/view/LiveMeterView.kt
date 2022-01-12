package spp.processor.live.impl.view

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import kotlinx.datetime.Clock
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.minus
import org.apache.skywalking.oap.server.core.exporter.ExportEvent
import org.apache.skywalking.oap.server.core.exporter.MetricValuesExportService
import org.slf4j.LoggerFactory
import spp.processor.InstrumentProcessorVerticle.Companion.liveInstrumentProcessor
import spp.processor.common.FeedbackProcessor
import spp.processor.live.impl.view.util.EntitySubscribersCache
import spp.processor.live.impl.view.util.MetricTypeSubscriptionCache
import spp.protocol.instrument.DurationStep
import spp.protocol.instrument.LiveSourceLocation

class LiveMeterView(private val subscriptionCache: MetricTypeSubscriptionCache) : MetricValuesExportService {

    companion object {
        private val log = LoggerFactory.getLogger(LiveMeterView::class.java)
    }

    override fun export(event: ExportEvent) {
        if (event.type == ExportEvent.EventType.INCREMENT) return
        val metricName = event.metrics.javaClass.simpleName
        if (!metricName.startsWith("spp_")) return
        if (log.isTraceEnabled) log.trace("Processing exported meter event: {}", event)

        val subbedMetrics = subscriptionCache[metricName]
        if (subbedMetrics != null) {
            //todo: location should be coming from subscription, as is functions as service/serviceInstance wildcard
            val location = LiveSourceLocation("", 0)
            sendMeterEvent(metricName, location, subbedMetrics, event.metrics.timeBucket)
        }
    }

    fun sendMeterEvent(
        metricName: String,
        location: LiveSourceLocation,
        subbedMetrics: EntitySubscribersCache,
        timeBucket: Long
    ) {
        val metricFutures = mutableListOf<Future<JsonObject>>()
        val minutePromise = Promise.promise<JsonObject>()
        liveInstrumentProcessor.getLiveMeterMetrics(
            metricName,
            location,
            Clock.System.now().minus(1, DateTimeUnit.MINUTE),
            Clock.System.now(),
            DurationStep.MINUTE,
            minutePromise
        )
        metricFutures.add(minutePromise.future())

        val hourPromise = Promise.promise<JsonObject>()
        liveInstrumentProcessor.getLiveMeterMetrics(
            metricName,
            location,
            Clock.System.now().minus(1, DateTimeUnit.HOUR),
            Clock.System.now(),
            DurationStep.HOUR,
            hourPromise
        )
        metricFutures.add(hourPromise.future())

        val dayPromise = Promise.promise<JsonObject>()
        liveInstrumentProcessor.getLiveMeterMetrics(
            metricName,
            location,
            Clock.System.now().minus(24, DateTimeUnit.HOUR),
            Clock.System.now(),
            DurationStep.DAY,
            dayPromise
        )
        metricFutures.add(dayPromise.future())

        CompositeFuture.all(metricFutures as List<Future<JsonObject>>).onComplete {
            if (it.succeeded()) {
                val minute = minutePromise.future().result()
                val hour = hourPromise.future().result()
                val day = dayPromise.future().result()

                subbedMetrics.values.flatten().forEach {
                    FeedbackProcessor.vertx.eventBus().send(
                        it.consumer.address(),
                        JsonObject()
                            .put("last_minute", minute.getJsonArray("values").getValue(0))
                            .put("last_hour", hour.getJsonArray("values").getValue(0))
                            .put("last_day", day.getJsonArray("values").getValue(0))
                            .put("timeBucket", timeBucket)
                            .put("multiMetrics", false)
                    )
                }
            } else {
                log.error("Failed to get live meter metrics", it.cause())
            }
        }
    }
}
