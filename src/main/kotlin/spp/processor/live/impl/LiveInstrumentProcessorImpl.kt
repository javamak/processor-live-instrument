package spp.processor.live.impl

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.datetime.Instant
import kotlinx.datetime.toJavaInstant
import org.apache.skywalking.oap.meter.analyzer.MetricConvert
import org.apache.skywalking.oap.server.analyzer.module.AnalyzerModule
import org.apache.skywalking.oap.server.analyzer.provider.meter.config.MeterConfig
import org.apache.skywalking.oap.server.analyzer.provider.meter.process.IMeterProcessService
import org.apache.skywalking.oap.server.analyzer.provider.meter.process.MeterProcessService
import org.apache.skywalking.oap.server.core.CoreModule
import org.apache.skywalking.oap.server.core.analysis.meter.MeterSystem
import org.apache.skywalking.oap.server.core.query.MetricsQueryService
import org.apache.skywalking.oap.server.core.query.enumeration.Scope
import org.apache.skywalking.oap.server.core.query.enumeration.Step
import org.apache.skywalking.oap.server.core.query.input.Duration
import org.apache.skywalking.oap.server.core.query.input.Entity
import org.apache.skywalking.oap.server.core.query.input.MetricsCondition
import org.apache.skywalking.oap.server.core.storage.StorageModule
import org.apache.skywalking.oap.server.core.storage.query.IMetadataQueryDAO
import org.joor.Reflect
import org.slf4j.LoggerFactory
import spp.processor.InstrumentProcessor
import spp.processor.common.SkyWalkingStorage.Companion.METRIC_PREFIX
import spp.processor.live.LiveInstrumentProcessor
import spp.protocol.instrument.DurationStep
import spp.protocol.instrument.LiveSourceLocation
import spp.protocol.instrument.meter.LiveMeter
import spp.protocol.instrument.meter.MeterType
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class LiveInstrumentProcessorImpl : CoroutineVerticle(), LiveInstrumentProcessor {

    companion object {
        private val log = LoggerFactory.getLogger(LiveInstrumentProcessorImpl::class.java)
    }

    private lateinit var metricsQueryService: MetricsQueryService
    private lateinit var metadata: IMetadataQueryDAO
    private lateinit var meterSystem: MeterSystem
    private lateinit var meterProcessService: MeterProcessService

    override suspend fun start() {
        log.info("Starting LiveInstrumentProcessorImpl")
        InstrumentProcessor.module!!.find(StorageModule.NAME).provider().apply {
            metadata = getService(IMetadataQueryDAO::class.java)
        }
        InstrumentProcessor.module!!.find(CoreModule.NAME).provider().apply {
            metricsQueryService = getService(MetricsQueryService::class.java)
            meterSystem = getService(MeterSystem::class.java)
        }
        InstrumentProcessor.module!!.find(AnalyzerModule.NAME).provider().apply {
            meterProcessService = getService(IMeterProcessService::class.java) as MeterProcessService
        }
    }

    override suspend fun stop() {
        log.info("Stopping LiveInstrumentProcessorImpl")
    }

    override fun setupLiveMeter(liveMeter: LiveMeter, handler: Handler<AsyncResult<JsonObject>>) {
        val meterConfig = MeterConfig()
        when (liveMeter.meterType) {
            MeterType.COUNT -> {
                meterConfig.metricPrefix = METRIC_PREFIX
                meterConfig.metricsRules = mutableListOf(
                    MeterConfig.Rule().apply {
                        val idVariable = liveMeter.toMetricIdWithoutPrefix()
                        name = idVariable
                        exp = "($idVariable.sum(['service', 'instance']).downsampling(SUM)).instance(['service'], ['instance'])"
                    }
                )
            }
            MeterType.GAUGE -> {
                meterConfig.metricPrefix = METRIC_PREFIX
                meterConfig.metricsRules = mutableListOf(
                    MeterConfig.Rule().apply {
                        val idVariable = liveMeter.toMetricIdWithoutPrefix()
                        name = idVariable
                        exp = "($idVariable.downsampling(LATEST)).instance(['service'], ['instance'])"
                    }
                )
            }
            MeterType.HISTOGRAM -> {
                meterConfig.metricPrefix = METRIC_PREFIX
                meterConfig.metricsRules = mutableListOf(
                    MeterConfig.Rule().apply {
                        val idVariable = liveMeter.toMetricIdWithoutPrefix()
                        name = idVariable
                        exp = "($idVariable.sum(['le', 'service', 'instance']).increase('PT5M').histogram().histogram_percentile([50,70,90,99])).instance(['service'], ['instance'])"
                    }
                )
            }
            else -> throw UnsupportedOperationException("Unsupported meter type: ${liveMeter.meterType}")
        }
        meterProcessService.converts().add(MetricConvert(meterConfig, meterSystem))
        handler.handle(Future.succeededFuture(JsonObject()))
    }

    override fun getLiveMeterMetrics(
        liveMeter: LiveMeter,
        start: Instant,
        stop: Instant,
        step: DurationStep,
        handler: Handler<AsyncResult<JsonObject>>
    ) {
        getLiveMeterMetrics(liveMeter.toMetricId(), liveMeter.location, start, stop, step, handler)
    }

    fun getLiveMeterMetrics(
        metricId: String,
        location: LiveSourceLocation,
        start: Instant,
        stop: Instant,
        step: DurationStep,
        handler: Handler<AsyncResult<JsonObject>>
    ) {
        log.debug("Getting live meter metrics. Metric id: {}", metricId)
        val services = metadata.getAllServices(location.service ?: "")
        if (services.isEmpty()) {
            log.info("No services found")
            handler.handle(Future.succeededFuture(JsonObject().put("values", JsonArray())))
            return
        }

        val values = mutableListOf<Any>()
        services.forEach { service ->
            val instances = metadata.getServiceInstances(
                start.toEpochMilliseconds(), stop.toEpochMilliseconds(), service.id
            )
            if (instances.isEmpty()) {
                log.info("No instances found for service: ${service.id}")
                return@forEach
            }

            instances.forEach { instance ->
                val serviceInstance = location.serviceInstance
                if (serviceInstance != null && serviceInstance != instance.name) {
                    return@forEach
                }

                val condition = MetricsCondition().apply {
                    name = metricId
                    entity = Entity().apply {
                        setScope(Scope.ServiceInstance)
                        setNormal(true)
                        setServiceName(service.name)
                        setServiceInstanceName(instance.name)
                    }
                }
                if (metricId.contains("histogram")) {
                    val value = metricsQueryService.readHeatMap(condition, Duration().apply {
                        Reflect.on(this).set(
                            "start",
                            DateTimeFormatter.ofPattern(step.pattern).withZone(ZoneOffset.UTC)
                                .format(start.toJavaInstant())
                        )
                        Reflect.on(this).set(
                            "end",
                            DateTimeFormatter.ofPattern(step.pattern).withZone(ZoneOffset.UTC)
                                .format(stop.toJavaInstant())
                        )
                        Reflect.on(this).set("step", Step.valueOf(step.name))
                    })
                    values.add(value)
                } else {
                    val value = metricsQueryService.readMetricsValue(condition, Duration().apply {
                        Reflect.on(this).set(
                            "start",
                            DateTimeFormatter.ofPattern(step.pattern).withZone(ZoneOffset.UTC)
                                .format(start.toJavaInstant())
                        )
                        Reflect.on(this).set(
                            "end",
                            DateTimeFormatter.ofPattern(step.pattern).withZone(ZoneOffset.UTC)
                                .format(stop.toJavaInstant())
                        )
                        Reflect.on(this).set("step", Step.valueOf(step.name))
                    })
                    values.add(value)
                }
            }
        }
        handler.handle(Future.succeededFuture(JsonObject().put("values", JsonArray(values))))
    }
}
