package spp.processor.live.impl.view

import org.apache.skywalking.oap.server.core.exporter.ExportEvent
import org.apache.skywalking.oap.server.core.exporter.MetricValuesExportService
import org.slf4j.LoggerFactory
import spp.processor.live.impl.view.util.MetricTypeSubscriptionCache

class LiveMeterView(private val subscriptionCache: MetricTypeSubscriptionCache) :
    AbstractLiveView(), MetricValuesExportService {

    companion object {
        private val log = LoggerFactory.getLogger(LiveMeterView::class.java)
    }

    override fun export(event: ExportEvent) {
        val metricName = event.metrics.javaClass.simpleName
        if (!metricName.startsWith("spp_")) return
        if (log.isTraceEnabled) log.trace("Processing exported meter event: {}", event)

        val subbedArtifacts = subscriptionCache[metricName]
        if (subbedArtifacts != null) {
            handleEvent(subbedArtifacts[metricName]!!, event)
        }
    }
}
