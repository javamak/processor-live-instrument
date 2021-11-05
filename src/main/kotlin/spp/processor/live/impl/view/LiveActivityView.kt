package spp.processor.live.impl.view

import org.apache.skywalking.oap.server.core.analysis.metrics.WithMetadata
import org.apache.skywalking.oap.server.core.exporter.ExportEvent
import org.apache.skywalking.oap.server.core.exporter.MetricValuesExportService
import org.slf4j.LoggerFactory
import spp.processor.live.impl.view.util.EntityNaming
import spp.processor.live.impl.view.util.MetricTypeSubscriptionCache

class LiveActivityView(private val subscriptionCache: MetricTypeSubscriptionCache) :
    AbstractLiveView(), MetricValuesExportService {

    companion object {
        private val log = LoggerFactory.getLogger(LiveActivityView::class.java)
    }

    override fun export(event: ExportEvent) {
        if (event.metrics !is WithMetadata) return
        val metadata = (event.metrics as WithMetadata).meta
        val entityName = EntityNaming.getEntityName(metadata)
        if (entityName.isNullOrEmpty()) return
        if (event.type != ExportEvent.EventType.TOTAL) return
        if (log.isTraceEnabled) log.trace("Processing exported event: {}", event)

        val metricName = metadata.metricsName
        val subbedArtifacts = subscriptionCache[metricName]
        if (subbedArtifacts != null) {
            val subs = subbedArtifacts[entityName] ?: return
            handleEvent(subs, event)
        }
    }
}
