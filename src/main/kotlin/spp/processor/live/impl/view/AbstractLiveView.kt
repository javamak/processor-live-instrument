package spp.processor.live.impl.view

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.apache.skywalking.oap.server.core.analysis.metrics.WithMetadata
import org.apache.skywalking.oap.server.core.exporter.ExportEvent
import spp.processor.common.FeedbackProcessor
import spp.processor.live.impl.view.util.EntityNaming
import spp.processor.live.impl.view.util.ViewSubscriber

abstract class AbstractLiveView {

    fun handleEvent(subs: Set<ViewSubscriber>, event: ExportEvent) {
        val jsonEvent = JsonObject.mapFrom(event)
        subs.forEach { sub ->
            var hasAllEvents = false
            if (sub.subscription.liveViewConfig.viewMetrics.size > 1) {
                if (sub.waitingEvents.isEmpty()) {
                    sub.waitingEvents.add(event)
                } else if (sub.subscription.liveViewConfig.viewMetrics.size != sub.waitingEvents.size) {
                    if (sub.waitingEvents.first().metrics.timeBucket == event.metrics.timeBucket) {
                        sub.waitingEvents.removeIf { it.metrics::class.java == event.metrics::class.java }
                        sub.waitingEvents.add(event)
                        if (sub.subscription.liveViewConfig.viewMetrics.size == sub.waitingEvents.size) {
                            hasAllEvents = true
                        }
                    } else if (event.metrics.timeBucket > sub.waitingEvents.first().metrics.timeBucket) {
                        //on new time before received all events for current time
                        sub.waitingEvents.clear()
                        sub.waitingEvents.add(event)
                    }
                }
            } else {
                hasAllEvents = true
            }

            if (hasAllEvents && System.currentTimeMillis() - sub.lastUpdated >= sub.subscription.liveViewConfig.refreshRateLimit) {
                sub.lastUpdated = System.currentTimeMillis()

                if (sub.waitingEvents.isNotEmpty()) {
                    val multiMetrics = JsonArray()
                    sub.waitingEvents.forEach {
                        multiMetrics.add(
                            JsonObject.mapFrom(it).getJsonObject("metrics")
                                .put("artifactQualifiedName", JsonObject.mapFrom(sub.subscription.artifactQualifiedName))
                                .put("entityId", EntityNaming.getEntityName((it.metrics as WithMetadata).meta))
                        )
                    }
                    sub.waitingEvents.clear()

                    FeedbackProcessor.vertx.eventBus().send(
                        sub.consumer.address(),
                        JsonObject().put("metrics", multiMetrics).put("multiMetrics", true)
                    )
                } else {
                    FeedbackProcessor.vertx.eventBus().send(
                        sub.consumer.address(),
                        jsonEvent.getJsonObject("metrics").put("multiMetrics", false)
                    )
                }
            }
        }
    }
}
