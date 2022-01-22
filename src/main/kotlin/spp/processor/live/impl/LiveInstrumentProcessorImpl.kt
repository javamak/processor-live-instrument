package spp.processor.live.impl

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.eventbus.ReplyFailure
import io.vertx.core.eventbus.impl.MessageImpl
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.impl.jose.JWT
import io.vertx.ext.bridge.BridgeEventType
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameHelper
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.datetime.toJavaInstant
import mu.KotlinLogging
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
import spp.processor.InstrumentProcessor
import spp.processor.common.FeedbackProcessor
import spp.processor.common.SkyWalkingStorage.Companion.METRIC_PREFIX
import spp.protocol.SourceMarkerServices
import spp.protocol.artifact.exception.LiveStackTrace
import spp.protocol.error.MissingRemoteException
import spp.protocol.instrument.*
import spp.protocol.instrument.breakpoint.LiveBreakpoint
import spp.protocol.instrument.breakpoint.event.LiveBreakpointHit
import spp.protocol.instrument.breakpoint.event.LiveBreakpointRemoved
import spp.protocol.instrument.log.LiveLog
import spp.protocol.instrument.log.event.LiveLogHit
import spp.protocol.instrument.log.event.LiveLogRemoved
import spp.protocol.instrument.meter.LiveMeter
import spp.protocol.instrument.meter.MeterType
import spp.protocol.instrument.meter.event.LiveMeterRemoved
import spp.protocol.instrument.span.LiveSpan
import spp.protocol.instrument.span.event.LiveSpanRemoved
import spp.protocol.platform.PlatformAddress
import spp.protocol.platform.client.ActiveProbe
import spp.protocol.platform.error.EventBusUtil
import spp.protocol.probe.ProbeAddress
import spp.protocol.probe.command.LiveInstrumentCommand
import spp.protocol.probe.command.LiveInstrumentContext
import spp.protocol.processor.ProcessorAddress
import spp.protocol.service.live.LiveInstrumentService
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class LiveInstrumentProcessorImpl : CoroutineVerticle(), LiveInstrumentService {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private lateinit var metricsQueryService: MetricsQueryService
    private lateinit var metadata: IMetadataQueryDAO
    private lateinit var meterSystem: MeterSystem
    private lateinit var meterProcessService: MeterProcessService

    override suspend fun start() {
        log.info("Starting LiveInstrumentProcessorImpl")
//        InstrumentProcessor.module!!.find(StorageModule.NAME).provider().apply {
//            metadata = getService(IMetadataQueryDAO::class.java)
//        }
//        InstrumentProcessor.module!!.find(CoreModule.NAME).provider().apply {
//            metricsQueryService = getService(MetricsQueryService::class.java)
//            meterSystem = getService(MeterSystem::class.java)
//        }
//        InstrumentProcessor.module!!.find(AnalyzerModule.NAME).provider().apply {
//            meterProcessService = getService(IMeterProcessService::class.java) as MeterProcessService
//        }

        vertx.setPeriodic(TimeUnit.SECONDS.toMillis(1)) {
            if (liveInstruments.isNotEmpty()) {
                liveInstruments.forEach {
                    if (it.instrument.pending
                        && it.instrument.expiresAt != null
                        && it.instrument.expiresAt!! <= System.currentTimeMillis()
                    ) {
                        removeLiveInstrument(it.selfId, it.accessToken, it)
                    }
                }
            }
        }

        //send active instruments on probe connection
        vertx.eventBus().consumer<JsonObject>(ProbeAddress.REMOTE_REGISTERED.address) {
            //todo: impl batch instrument add
            //todo: more efficient to just send batch add to specific probe instead of publish to all per connection
            //todo: probably need to redo pending boolean. it doesn't make sense here since pending just means
            // it has been applied to any instrument at all at any point
            val remote = it.body().getString("address").substringBefore(":")
            if (remote == ProbeAddress.LIVE_BREAKPOINT_REMOTE.address) {
                log.debug("Live breakpoint remote registered. Sending active live breakpoints")
                liveInstruments.filter { it.instrument is LiveBreakpoint }.forEach {
                    addBreakpoint(it.selfId, it.accessToken, it.instrument as LiveBreakpoint, false)
                }
            }
            if (remote == ProbeAddress.LIVE_LOG_REMOTE.address) {
                log.debug("Live log remote registered. Sending active live logs")
                liveInstruments.filter { it.instrument is LiveLog }.forEach {
                    addLog(it.selfId, it.accessToken, it.instrument as LiveLog, false)
                }
            }
            if (remote == ProbeAddress.LIVE_METER_REMOTE.address) {
                log.debug("Live meter remote registered. Sending active live meters")
                liveInstruments.filter { it.instrument is LiveMeter }.forEach {
                    addMeter(it.selfId, it.accessToken, it.instrument as LiveMeter, false)
                }
            }
            if (remote == ProbeAddress.LIVE_SPAN_REMOTE.address) {
                log.debug("Live span remote registered. Sending active live spans")
                liveInstruments.filter { it.instrument is LiveSpan }.forEach {
                    addSpan(it.selfId, it.accessToken, it.instrument as LiveSpan, false)
                }
            }
        }

        listenForLiveBreakpoints()
        listenForLiveLogs()
        listenForLiveMeters()
        listenForLiveSpans()
    }

    override suspend fun stop() {
        log.info("Stopping LiveInstrumentProcessorImpl")
    }

    override fun addLiveInstrument(instrument: LiveInstrument, handler: Handler<AsyncResult<LiveInstrument>>) {
        var accessToken: String? = null
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$2").headers().let {
            if (it.contains("auth-token")) {
                accessToken = it.get("auth-token")
                JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id")
            } else {
                it.get("developer_id")
            }
        }
        log.info(
            "Received add live instrument request. Developer: {} - Location: {}",
            selfId, instrument.location.let { it.source + ":" + it.line }
        )

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                when (instrument) {
                    is LiveBreakpoint -> {
                        val pendingBp = if (instrument.id == null) {
                            instrument.copy(id = UUID.randomUUID().toString())
                        } else {
                            instrument
                        }.copy(
                            pending = true,
                            applied = false,
                            meta = instrument.meta.toMutableMap().apply {
                                put("created_at", System.currentTimeMillis().toString())
                                put("created_by", selfId)
                                put("hit_count", AtomicInteger())
                            }
                        )

                        if (pendingBp.applyImmediately) {
                            addApplyImmediatelyHandler(pendingBp.id!!, handler)
                            addBreakpoint(selfId, accessToken, pendingBp)
                        } else {
                            handler.handle(addBreakpoint(selfId, accessToken, pendingBp))
                        }
                    }
                    is LiveLog -> {
                        val pendingLog = if (instrument.id == null) {
                            instrument.copy(id = UUID.randomUUID().toString())
                        } else {
                            instrument
                        }.copy(
                            pending = true,
                            applied = false,
                            meta = instrument.meta.toMutableMap().apply {
                                put("created_at", System.currentTimeMillis().toString())
                                put("created_by", selfId)
                                put("hit_count", AtomicInteger())
                            }
                        )

                        if (pendingLog.applyImmediately) {
                            addApplyImmediatelyHandler(pendingLog.id!!, handler)
                            addLog(selfId, accessToken, pendingLog)
                        } else {
                            handler.handle(addLog(selfId, accessToken, pendingLog))
                        }
                    }
                    is LiveMeter -> {
                        val pendingMeter = if (instrument.id == null) {
                            instrument.copy(id = UUID.randomUUID().toString())
                        } else {
                            instrument
                        }.copy(
                            pending = true,
                            applied = false,
                            meta = instrument.meta.toMutableMap().apply {
                                put("created_at", System.currentTimeMillis().toString())
                                put("created_by", selfId)
                            }
                        )

                        setupLiveMeter(pendingMeter)
                        if (pendingMeter.applyImmediately) {
                            addApplyImmediatelyHandler(pendingMeter.id!!, handler)
                            addMeter(selfId, accessToken, pendingMeter)
                        } else {
                            handler.handle(addMeter(selfId, accessToken, pendingMeter))
                        }
                    }
                    is LiveSpan -> {
                        val pendingSpan = if (instrument.id == null) {
                            instrument.copy(id = UUID.randomUUID().toString())
                        } else {
                            instrument
                        }.copy(
                            pending = true,
                            applied = false,
                            meta = instrument.meta.toMutableMap().apply {
                                put("created_at", System.currentTimeMillis().toString())
                                put("created_by", selfId)
                            }
                        )

                        if (pendingSpan.applyImmediately) {
                            addApplyImmediatelyHandler(pendingSpan.id!!, handler)
                            addSpan(selfId, accessToken, pendingSpan)
                        } else {
                            handler.handle(addSpan(selfId, accessToken, pendingSpan))
                        }
                    }
                    else -> {
                        handler.handle(Future.failedFuture(IllegalArgumentException("Unknown live instrument type")))
                    }
                }
            } catch (throwable: Throwable) {
                log.warn("Add live instrument failed", throwable)
                handler.handle(Future.failedFuture(throwable))
            }
        }
    }

    override fun addLiveInstruments(batch: LiveInstrumentBatch, handler: Handler<AsyncResult<List<LiveInstrument>>>) {
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$2").headers().let {
            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
                .getJsonObject("payload").getString("developer_id")
        }
        log.info(
            "Received add live instrument batch request. Developer: {} - Location(s): {}",
            selfId, batch.instruments.map { it.location.let { it.source + ":" + it.line } }
        )

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                val results = mutableListOf<LiveInstrument>()
                batch.instruments.forEach {
                    val promise = Promise.promise<LiveInstrument>()
                    addLiveInstrument(it, promise)
                    results.add(promise.future().await())
                }
                handler.handle(Future.succeededFuture(results))
            } catch (throwable: Throwable) {
                log.warn("Add live instruments failed", throwable)
                handler.handle(Future.failedFuture(throwable))
            }
        }
    }

    override fun getLiveInstruments(handler: Handler<AsyncResult<List<LiveInstrument>>>) {
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$2").headers().let {
            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
                .getJsonObject("payload").getString("developer_id")
        }
        log.info("Received get live instruments request. Developer: {}", selfId)

        handler.handle(Future.succeededFuture(getLiveInstruments()))
    }

    override fun removeLiveInstrument(id: String, handler: Handler<AsyncResult<LiveInstrument?>>) {
        var accessToken: String? = null
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$2").headers().let {
            if (it.contains("auth-token")) {
                accessToken = it.get("auth-token")
                JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id")
            } else {
                it.get("developer_id")
            }
        }
        log.info("Received remove live instrument request. Developer: {} - Id: {}", selfId, id)

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                handler.handle(removeLiveInstrument(selfId, accessToken, id))
            } catch (throwable: Throwable) {
                log.warn("Remove live instrument failed", throwable)
                handler.handle(Future.failedFuture(throwable))
            }
        }
    }

    override fun removeLiveInstruments(
        location: LiveSourceLocation, handler: Handler<AsyncResult<List<LiveInstrument>>>
    ) {
        var accessToken: String? = null
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$2").headers().let {
            if (it.contains("auth-token")) {
                accessToken = it.get("auth-token")
                JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id")
            } else {
                it.get("developer_id")
            }
        }
        log.info("Received remove live instruments request. Developer: {} - Location: {}", selfId, location)

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                val breakpointsResult = removeBreakpoints(selfId, accessToken, location)
                val logsResult = removeLogs(selfId, accessToken, location)
                val metersResult = removeMeters(selfId, accessToken, location)

                when {
                    breakpointsResult.failed() -> handler.handle(Future.failedFuture(breakpointsResult.cause()))
                    logsResult.failed() -> handler.handle(Future.failedFuture(logsResult.cause()))
                    metersResult.failed() -> handler.handle(Future.failedFuture(metersResult.cause()))
                    else -> handler.handle(
                        Future.succeededFuture(
                            breakpointsResult.result() + logsResult.result() + metersResult.result()
                        )
                    )
                }
            } catch (throwable: Throwable) {
                log.warn("Remove live instruments failed", throwable)
                handler.handle(Future.failedFuture(throwable))
            }
        }
    }

    override fun getLiveInstrumentById(id: String, handler: Handler<AsyncResult<LiveInstrument?>>) {
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$2").headers().let {
            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
                .getJsonObject("payload").getString("developer_id")
        }
        log.info("Received get live instrument by id request. Developer: {} - Id: {}", selfId, id)

        handler.handle(Future.succeededFuture(getLiveInstrumentById(id)))
    }

    override fun getLiveInstrumentsByIds(ids: List<String>, handler: Handler<AsyncResult<List<LiveInstrument>>>) {
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$2").headers().let {
            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
                .getJsonObject("payload").getString("developer_id")
        }
        log.info("Received get live instruments by ids request. Developer: {} - Ids: {}", selfId, ids)

        handler.handle(Future.succeededFuture(getLiveInstrumentsByIds(ids)))
    }

    override fun getLiveBreakpoints(handler: Handler<AsyncResult<List<LiveBreakpoint>>>) {
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
                .getJsonObject("payload").getString("developer_id")
        }
        log.info("Received get live breakpoints request. Developer: {}", selfId)

        handler.handle(Future.succeededFuture(getActiveLiveBreakpoints()))
    }

    override fun getLiveLogs(handler: Handler<AsyncResult<List<LiveLog>>>) {
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
                .getJsonObject("payload").getString("developer_id")
        }
        log.info("Received get live logs request. Developer: {}", selfId)

        handler.handle(Future.succeededFuture(getActiveLiveLogs()))
    }

    override fun getLiveMeters(handler: Handler<AsyncResult<List<LiveMeter>>>) {
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
                .getJsonObject("payload").getString("developer_id")
        }
        log.info("Received get live meters request. Developer: {}", selfId)

        handler.handle(Future.succeededFuture(getActiveLiveMeters()))
    }

    override fun getLiveSpans(handler: Handler<AsyncResult<List<LiveSpan>>>) {
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
                .getJsonObject("payload").getString("developer_id")
        }
        log.info("Received get live spans request. Developer: {}", selfId)

        handler.handle(Future.succeededFuture(getActiveLiveSpans()))
    }

//    fun clearAllLiveInstruments() {
//        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$2").headers().let {
//            it.get("developer_id") ?: JWT.parse(it.get("auth-token"))
//                .getJsonObject("payload").getString("developer_id")
//        }
//        log.info("Received clear live instruments request. Developer: {}", selfId)
//
//        clearAllLiveInstruments(selfId)
//    }

    override fun clearLiveInstruments(handler: Handler<AsyncResult<Boolean>>) {
        var accessToken: String? = null
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            if (it.contains("auth-token")) {
                accessToken = it.get("auth-token")
                JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id")
            } else {
                it.get("developer_id")
            }
        }
        log.info("Received clear live instruments request. Developer: {}", selfId)

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                clearLiveInstruments(selfId, accessToken, handler)
            } catch (throwable: Throwable) {
                log.warn("Clear live instruments failed", throwable)
                handler.handle(Future.failedFuture(throwable))
            }
        }
    }

    override fun clearLiveBreakpoints(handler: Handler<AsyncResult<Boolean>>) {
        var accessToken: String? = null
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            if (it.contains("auth-token")) {
                accessToken = it.get("auth-token")
                JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id")
            } else {
                it.get("developer_id")
            }
        }
        log.info("Received clear live breakpoints request. Developer: {}", selfId)

        clearLiveBreakpoints(selfId, accessToken, handler)
    }

    override fun clearLiveLogs(handler: Handler<AsyncResult<Boolean>>) {
        var accessToken: String? = null
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            if (it.contains("auth-token")) {
                accessToken = it.get("auth-token")
                JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id")
            } else {
                it.get("developer_id")
            }
        }
        log.info("Received clear live logs request. Developer: {}", selfId)

        clearLiveLogs(selfId, accessToken, handler)
    }

    override fun clearLiveMeters(handler: Handler<AsyncResult<Boolean>>) {
        var accessToken: String? = null
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            if (it.contains("auth-token")) {
                accessToken = it.get("auth-token")
                JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id")
            } else {
                it.get("developer_id")
            }
        }
        log.info("Received clear live meters request. Developer: {}", selfId)

        clearLiveMeters(selfId, accessToken, handler)
    }

    override fun clearLiveSpans(handler: Handler<AsyncResult<Boolean>>) {
        var accessToken: String? = null
        val selfId = Reflect.on(handler).get<MessageImpl<*, *>>("arg\$1").headers().let {
            if (it.contains("auth-token")) {
                accessToken = it.get("auth-token")
                JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id")
            } else {
                it.get("developer_id")
            }
        }
        log.info("Received clear live spans request. Developer: {}", selfId)

        clearLiveSpans(selfId, accessToken, handler)
    }

    private suspend fun setupLiveMeter(liveMeter: LiveMeter) {
        log.info("Setting up live meter: $liveMeter")

        val async = Promise.promise<JsonObject>()
        setupLiveMeter(liveMeter, async)
        async.future().await()
    }

    private val liveInstruments = Collections.newSetFromMap(ConcurrentHashMap<DeveloperInstrument, Boolean>())
    private val waitingApply = ConcurrentHashMap<String, Handler<AsyncResult<DeveloperInstrument>>>()

    private fun listenForLiveBreakpoints() {
        vertx.eventBus().consumer<JsonObject>(PlatformAddress.LIVE_BREAKPOINT_APPLIED.address) {
            if (log.isTraceEnabled) log.trace("Got live breakpoint applied: {}", it.body())
            val bp = Json.decodeValue(it.body().toString(), LiveBreakpoint::class.java)
            liveInstruments.forEach {
                if (it.instrument.id == bp.id) {
                    log.info("Live breakpoint applied. Id: {}", it.instrument.id)
                    val appliedBp = (it.instrument as LiveBreakpoint).copy(
                        applied = true,
                        pending = false
                    )
                    (appliedBp.meta as MutableMap<String, Any>)["applied_at"] = System.currentTimeMillis().toString()

                    val devInstrument = DeveloperInstrument(it.selfId, it.accessToken, appliedBp)
                    liveInstruments.remove(it)
                    liveInstruments.add(devInstrument)

                    waitingApply.remove(appliedBp.id)?.handle(Future.succeededFuture(devInstrument))

                    vertx.eventBus().publish(
                        SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                        JsonObject.mapFrom(
                            LiveInstrumentEvent(
                                LiveInstrumentEventType.BREAKPOINT_APPLIED,
                                Json.encode(appliedBp)
                            )
                        )
                    )
                    if (log.isTraceEnabled) log.trace("Published live breakpoint applied")
                    return@forEach
                }
            }
        }
        vertx.eventBus().consumer<JsonObject>(PlatformAddress.LIVE_BREAKPOINT_REMOVED.address) {
            if (log.isTraceEnabled) log.trace("Got live breakpoint removed: {}", it.body())
            val bpCommand = it.body().getString("command")
            val bpData = if (bpCommand != null) {
                val command = Json.decodeValue(bpCommand, LiveInstrumentCommand::class.java)
                JsonObject(command.context.liveInstruments[0]) //todo: check for multiple
            } else {
                JsonObject(it.body().getString("breakpoint"))
            }

            val instrumentRemoval = liveInstruments.find { find -> find.instrument.id == bpData.getString("id") }
            if (instrumentRemoval != null) {
                //publish remove command to all probes & markers
                removeLiveBreakpoint(
                    instrumentRemoval.selfId,
                    instrumentRemoval.accessToken,
                    Instant.fromEpochMilliseconds(it.body().getLong("occurredAt")),
                    instrumentRemoval.instrument as LiveBreakpoint,
                    it.body().getString("cause")
                )
            }
        }
        vertx.eventBus().consumer<JsonObject>(ProcessorAddress.BREAKPOINT_HIT.address) {
            if (log.isTraceEnabled) log.trace("Live breakpoint hit: {}", it.body())
            val bpHit = Json.decodeValue(it.body().toString(), LiveBreakpointHit::class.java)
            val instrument = getLiveInstrumentById(bpHit.breakpointId)
            if (instrument != null) {
                val instrumentMeta = instrument.meta as MutableMap<String, Any>
                if ((instrumentMeta["hit_count"] as AtomicInteger?)?.incrementAndGet() == 1) {
                    instrumentMeta["first_hit_at"] = System.currentTimeMillis().toString()
                }
                instrumentMeta["last_hit_at"] = System.currentTimeMillis().toString()
            }

            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(LiveInstrumentEvent(LiveInstrumentEventType.BREAKPOINT_HIT, Json.encode(bpHit)))
            )
            if (log.isTraceEnabled) log.trace("Published live breakpoint hit")
        }
    }

    private fun listenForLiveLogs() {
        vertx.eventBus().consumer<JsonObject>(ProcessorAddress.LOG_HIT.address) {
            if (log.isTraceEnabled) log.trace("Live log hit: {}", it.body())
            val logHit = Json.decodeValue(it.body().toString(), LiveLogHit::class.java)
            val instrument = getLiveInstrumentById(logHit.logId)
            if (instrument != null) {
                val instrumentMeta = instrument.meta as MutableMap<String, Any>
                if ((instrumentMeta["hit_count"] as AtomicInteger?)?.incrementAndGet() == 1) {
                    instrumentMeta["first_hit_at"] = System.currentTimeMillis().toString()
                }
                instrumentMeta["last_hit_at"] = System.currentTimeMillis().toString()
            }

            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(LiveInstrumentEvent(LiveInstrumentEventType.LOG_HIT, it.body().toString()))
            )
            if (log.isTraceEnabled) log.trace("Published live log hit")
        }
        vertx.eventBus().consumer<JsonObject>(PlatformAddress.LIVE_LOG_APPLIED.address) {
            val liveLog = Json.decodeValue(it.body().toString(), LiveLog::class.java)
            liveInstruments.forEach {
                if (it.instrument.id == liveLog.id) {
                    log.info("Live log applied. Id: {}", it.instrument.id)
                    val appliedLog = (it.instrument as LiveLog).copy(
                        applied = true,
                        pending = false
                    )
                    (appliedLog.meta as MutableMap<String, Any>)["applied_at"] = System.currentTimeMillis().toString()

                    val devInstrument = DeveloperInstrument(it.selfId, it.accessToken, appliedLog)
                    liveInstruments.remove(it)
                    liveInstruments.add(devInstrument)

                    waitingApply.remove(appliedLog.id)?.handle(Future.succeededFuture(devInstrument))

                    vertx.eventBus().publish(
                        SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                        JsonObject.mapFrom(
                            LiveInstrumentEvent(
                                LiveInstrumentEventType.LOG_APPLIED,
                                Json.encode(appliedLog)
                            )
                        )
                    )
                    if (log.isTraceEnabled) log.trace("Published live log applied")
                    return@forEach
                }
            }
        }
        vertx.eventBus().consumer<JsonObject>(PlatformAddress.LIVE_LOG_REMOVED.address) {
            if (log.isTraceEnabled) log.trace("Got live log removed: {}", it.body())
            val logCommand = it.body().getString("command")
            val logData = if (logCommand != null) {
                val command = Json.decodeValue(logCommand, LiveInstrumentCommand::class.java)
                JsonObject(command.context.liveInstruments[0]) //todo: check for multiple
            } else {
                JsonObject(it.body().getString("log"))
            }

            val instrumentRemoval = liveInstruments.find { find -> find.instrument.id == logData.getString("id") }
            if (instrumentRemoval != null) {
                //publish remove command to all probes & markers
                removeLiveLog(
                    instrumentRemoval.selfId,
                    instrumentRemoval.accessToken,
                    Instant.fromEpochMilliseconds(it.body().getLong("occurredAt")),
                    instrumentRemoval.instrument as LiveLog,
                    it.body().getString("cause")
                )
            }
        }
    }

    private fun listenForLiveMeters() {
        vertx.eventBus().consumer<JsonObject>(PlatformAddress.LIVE_METER_APPLIED.address) {
            val liveMeter = Json.decodeValue(it.body().toString(), LiveMeter::class.java)
            liveInstruments.forEach {
                if (it.instrument.id == liveMeter.id) {
                    log.info("Live meter applied. Id: {}", it.instrument.id)
                    val appliedMeter = (it.instrument as LiveMeter).copy(
                        applied = true,
                        pending = false
                    )
                    (appliedMeter.meta as MutableMap<String, Any>)["applied_at"] = System.currentTimeMillis().toString()

                    val devInstrument = DeveloperInstrument(it.selfId, it.accessToken, appliedMeter)
                    liveInstruments.remove(it)
                    liveInstruments.add(devInstrument)

                    waitingApply.remove(appliedMeter.id)?.handle(Future.succeededFuture(devInstrument))

                    vertx.eventBus().publish(
                        SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                        JsonObject.mapFrom(
                            LiveInstrumentEvent(
                                LiveInstrumentEventType.METER_APPLIED,
                                Json.encode(appliedMeter)
                            )
                        )
                    )
                    if (log.isTraceEnabled) log.trace("Published live meter applied")
                    return@forEach
                }
            }
        }
        vertx.eventBus().consumer<JsonObject>(PlatformAddress.LIVE_METER_REMOVED.address) {
            if (log.isTraceEnabled) log.trace("Got live meter removed: {}", it.body())
            val meterCommand = it.body().getString("command")
            val meterData = if (meterCommand != null) {
                val command = Json.decodeValue(meterCommand, LiveInstrumentCommand::class.java)
                JsonObject(command.context.liveInstruments[0]) //todo: check for multiple
            } else {
                JsonObject(it.body().getString("meter"))
            }

            val instrumentRemoval = liveInstruments.find { find -> find.instrument.id == meterData.getString("id") }
            if (instrumentRemoval != null) {
                //publish remove command to all probes & markers
                removeLiveMeter(
                    instrumentRemoval.selfId,
                    instrumentRemoval.accessToken,
                    Instant.fromEpochMilliseconds(it.body().getLong("occurredAt")),
                    instrumentRemoval.instrument as LiveMeter,
                    it.body().getString("cause")
                )
            }
        }
    }

    private fun listenForLiveSpans() {
        vertx.eventBus().consumer<JsonObject>(PlatformAddress.LIVE_SPAN_APPLIED.address) {
            val liveSpan = Json.decodeValue(it.body().toString(), LiveSpan::class.java)
            liveInstruments.forEach {
                if (it.instrument.id == liveSpan.id) {
                    log.info("Live span applied. Id: {}", it.instrument.id)
                    val appliedSpan = (it.instrument as LiveSpan).copy(
                        applied = true,
                        pending = false
                    )
                    (appliedSpan.meta as MutableMap<String, Any>)["applied_at"] = System.currentTimeMillis().toString()

                    val devInstrument = DeveloperInstrument(it.selfId, it.accessToken, appliedSpan)
                    liveInstruments.remove(it)
                    liveInstruments.add(devInstrument)

                    waitingApply.remove(appliedSpan.id)?.handle(Future.succeededFuture(devInstrument))

                    vertx.eventBus().publish(
                        SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                        JsonObject.mapFrom(
                            LiveInstrumentEvent(
                                LiveInstrumentEventType.SPAN_APPLIED,
                                Json.encode(appliedSpan)
                            )
                        )
                    )
                    if (log.isTraceEnabled) log.trace("Published live span applied")
                    return@forEach
                }
            }
        }
        vertx.eventBus().consumer<JsonObject>(PlatformAddress.LIVE_SPAN_REMOVED.address) {
            if (log.isTraceEnabled) log.trace("Got live span removed: {}", it.body())
            val spanCommand = it.body().getString("command")
            val spanData = if (spanCommand != null) {
                val command = Json.decodeValue(spanCommand, LiveInstrumentCommand::class.java)
                JsonObject(command.context.liveInstruments[0]) //todo: check for multiple
            } else {
                JsonObject(it.body().getString("span"))
            }

            val instrumentRemoval = liveInstruments.find { find -> find.instrument.id == spanData.getString("id") }
            if (instrumentRemoval != null) {
                //publish remove command to all probes & markers
                removeLiveSpan(
                    instrumentRemoval.selfId,
                    instrumentRemoval.accessToken,
                    Instant.fromEpochMilliseconds(it.body().getLong("occurredAt")),
                    instrumentRemoval.instrument as LiveSpan,
                    it.body().getString("cause")
                )
            }
        }
    }

    fun addApplyImmediatelyHandler(instrumentId: String, handler: Handler<AsyncResult<LiveInstrument>>) {
        waitingApply[instrumentId] = Handler<AsyncResult<DeveloperInstrument>> {
            if (it.succeeded()) {
                handler.handle(Future.succeededFuture(it.result().instrument))
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        }
    }

    fun getLiveInstruments(): List<LiveInstrument> {
        return liveInstruments.map { it.instrument }.toList()
    }

    fun getActiveLiveBreakpoints(): List<LiveBreakpoint> {
        return liveInstruments.map { it.instrument }.filterIsInstance(LiveBreakpoint::class.java).filter { !it.pending }
    }

    fun getActiveLiveLogs(): List<LiveLog> {
        return liveInstruments.map { it.instrument }.filterIsInstance(LiveLog::class.java).filter { !it.pending }
    }

    fun getActiveLiveMeters(): List<LiveMeter> {
        return liveInstruments.map { it.instrument }.filterIsInstance(LiveMeter::class.java).filter { !it.pending }
    }

    fun getActiveLiveSpans(): List<LiveSpan> {
        return liveInstruments.map { it.instrument }.filterIsInstance(LiveSpan::class.java).filter { !it.pending }
    }

    fun addMeter(
        selfId: String, accessToken: String?, meter: LiveMeter, alertSubscribers: Boolean = true
    ): AsyncResult<LiveInstrument> {
        log.debug("Adding live meter: $meter")
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.ADD_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(meter)

        val devMeter = DeveloperInstrument(selfId, accessToken, meter)
        liveInstruments.add(devMeter)
        try {
            dispatchCommand(accessToken, ProbeAddress.LIVE_METER_REMOTE, meter.location, debuggerCommand)
        } catch (ex: ReplyException) {
            return if (ex.failureType() == ReplyFailure.NO_HANDLERS) {
                if (meter.applyImmediately) {
                    liveInstruments.remove(devMeter)
                    log.warn("Live meter failed due to missing remote(s)")
                    Future.failedFuture(MissingRemoteException(ProbeAddress.LIVE_METER_REMOTE.address).toEventBusException())
                } else {
                    log.info("Live meter pending application on probe connection")
                    Future.succeededFuture(meter)
                }
            } else {
                liveInstruments.remove(devMeter)
                log.warn("Failed to add live meter: Reason: {}", ex.message)
                Future.failedFuture(ex)
            }
        }

        if (alertSubscribers) {
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(LiveInstrumentEventType.METER_ADDED, Json.encode(meter))
                )
            )
        }
        return Future.succeededFuture(meter)
    }

    fun addBreakpoint(
        selfId: String, accessToken: String?, breakpoint: LiveBreakpoint, alertSubscribers: Boolean = true
    ): AsyncResult<LiveInstrument> {
        log.debug("Adding live breakpoint: $breakpoint")
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.ADD_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(breakpoint)

        val devBreakpoint = DeveloperInstrument(selfId, accessToken, breakpoint)
        liveInstruments.add(devBreakpoint)
        try {
            dispatchCommand(accessToken, ProbeAddress.LIVE_BREAKPOINT_REMOTE, breakpoint.location, debuggerCommand)
        } catch (ex: ReplyException) {
            return if (ex.failureType() == ReplyFailure.NO_HANDLERS) {
                if (breakpoint.applyImmediately) {
                    liveInstruments.remove(devBreakpoint)
                    log.warn("Live breakpoint failed due to missing remote(s)")
                    Future.failedFuture(MissingRemoteException(ProbeAddress.LIVE_BREAKPOINT_REMOTE.address).toEventBusException())
                } else {
                    log.info("Live breakpoint pending application on probe connection")
                    Future.succeededFuture(breakpoint)
                }
            } else {
                liveInstruments.remove(devBreakpoint)
                log.warn("Failed to add live breakpoint: Reason: {}", ex.message)
                Future.failedFuture(ex)
            }
        }

        if (alertSubscribers) {
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(LiveInstrumentEventType.BREAKPOINT_ADDED, Json.encode(breakpoint))
                )
            )
        }
        return Future.succeededFuture(breakpoint)
    }

    fun addSpan(
        selfId: String, accessToken: String?, span: LiveSpan, alertSubscribers: Boolean = true
    ): AsyncResult<LiveInstrument> {
        log.debug("Adding live span: $span")
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.ADD_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(span)

        val devSpan = DeveloperInstrument(selfId, accessToken, span)
        liveInstruments.add(devSpan)
        try {
            dispatchCommand(accessToken, ProbeAddress.LIVE_SPAN_REMOTE, span.location, debuggerCommand)
        } catch (ex: ReplyException) {
            return if (ex.failureType() == ReplyFailure.NO_HANDLERS) {
                if (span.applyImmediately) {
                    liveInstruments.remove(devSpan)
                    log.warn("Live span failed due to missing remote(s)")
                    Future.failedFuture(MissingRemoteException(ProbeAddress.LIVE_SPAN_REMOTE.address).toEventBusException())
                } else {
                    log.info("Live span pending application on probe connection")
                    Future.succeededFuture(span)
                }
            } else {
                liveInstruments.remove(devSpan)
                log.warn("Failed to add live span: Reason: {}", ex.message)
                Future.failedFuture(ex)
            }
        }

        if (alertSubscribers) {
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(LiveInstrumentEventType.SPAN_ADDED, Json.encode(span))
                )
            )
        }
        return Future.succeededFuture(span)
    }

    private fun dispatchCommand(
        accessToken: String?,
        address: ProbeAddress,
        location: LiveSourceLocation,
        debuggerCommand: LiveInstrumentCommand
    ) = GlobalScope.launch(vertx.dispatcher()) {
        val promise = Promise.promise<JsonArray>()
        InstrumentProcessor.requestEvent(
            vertx, SourceMarkerServices.Utilize.LIVE_SERVICE, JsonObject(),
            JsonObject().put("auth-token", accessToken).put("action", "getActiveProbes")
        ) {
            if (it.succeeded()) {
                promise.complete(it.result().getJsonArray("value"))
            } else {
                promise.fail(it.cause())
            }
        }
        val activeProbes = promise.future().await().map { Json.decodeValue(it.toString(), ActiveProbe::class.java) }

        val sendToProbes = activeProbes.filter {
            (location.service == null || it.meta["service"] == location.service) &&
                    (location.serviceInstance == null || it.meta["service_instance"] == location.serviceInstance)
        }
        if (sendToProbes.isEmpty()) {
            log.warn("No active probes found to receive command")
            return@launch
        } else {
            log.debug("Dispatching {} to {} probes", debuggerCommand.commandType, sendToProbes.size)
        }

        sendToProbes.forEach {
            log.trace("Dispatching command to probe: {}", it.probeId)
            FrameHelper.sendFrame(
                BridgeEventType.SEND.name.lowercase(),
                address.address + ":" + it.probeId, null, JsonObject(), true,
                JsonObject.mapFrom(debuggerCommand), FeedbackProcessor.tcpSocket
            )
        }
    }

    fun getLiveInstrumentById(id: String): LiveInstrument? {
        return liveInstruments.find { it.instrument.id == id }?.instrument
    }

    fun getLiveInstrumentsByIds(ids: List<String>): List<LiveInstrument> {
        return ids.mapNotNull { getLiveInstrumentById(it) }
    }

    fun addLog(
        selfId: String, accessToken: String?, liveLog: LiveLog, alertSubscribers: Boolean = true
    ): AsyncResult<LiveInstrument> {
        log.debug("Adding live log: $liveLog")
        val logCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.ADD_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        logCommand.context.addLiveInstrument(liveLog)

        val devLog = DeveloperInstrument(selfId, accessToken, liveLog)
        liveInstruments.add(devLog)
        try {
            dispatchCommand(accessToken, ProbeAddress.LIVE_LOG_REMOTE, liveLog.location, logCommand)
        } catch (ex: ReplyException) {
            return if (ex.failureType() == ReplyFailure.NO_HANDLERS) {
                if (liveLog.applyImmediately) {
                    liveInstruments.remove(devLog)
                    log.warn("Live log failed due to missing remote")
                    Future.failedFuture(MissingRemoteException(ProbeAddress.LIVE_LOG_REMOTE.address).toEventBusException())
                } else {
                    log.info("Live log pending application on probe connection")
                    Future.succeededFuture(liveLog)
                }
            } else {
                liveInstruments.remove(devLog)
                log.warn("Failed to add live log: Reason: {}", ex.message)
                Future.failedFuture(ex)
            }
        }

        if (alertSubscribers) {
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(LiveInstrumentEventType.LOG_ADDED, Json.encode(liveLog))
                )
            )
        }
        return Future.succeededFuture(liveLog)
    }

    private fun removeLiveBreakpoint(
        selfId: String, accessToken: String?,
        occurredAt: Instant, breakpoint: LiveBreakpoint, cause: String?
    ) {
        log.debug("Removing live breakpoint: ${breakpoint.id}")
        val devBreakpoint = DeveloperInstrument(selfId, accessToken, breakpoint)
        liveInstruments.remove(devBreakpoint)

        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(breakpoint)
        dispatchCommand(accessToken, ProbeAddress.LIVE_BREAKPOINT_REMOTE, breakpoint.location, debuggerCommand)

        val jvmCause = if (cause == null) null else LiveStackTrace.fromString(cause)
        val waitingHandler = waitingApply.remove(breakpoint.id)
        if (waitingHandler != null) {
            if (cause?.startsWith("EventBusException") == true) {
                val ebException = EventBusUtil.fromEventBusException(cause)
                waitingHandler.handle(Future.failedFuture(ebException))
            } else {
                TODO("$cause")
            }
        } else {
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(
                        LiveInstrumentEventType.BREAKPOINT_REMOVED,
                        //todo: could send whole breakpoint instead of just id
                        Json.encode(LiveBreakpointRemoved(breakpoint.id!!, occurredAt, jvmCause))
                    )
                )
            )
        }

        if (jvmCause != null) {
            log.warn("Publish live breakpoint removed. Cause: {}", jvmCause.message)
        } else {
            log.info("Published live breakpoint removed")
        }
    }

    private fun removeLiveLog(
        selfId: String, accessToken: String?,
        occurredAt: Instant, liveLog: LiveLog, cause: String?
    ) {
        log.debug("Removing live log: ${liveLog.id}")
        val devLog = DeveloperInstrument(selfId, accessToken, liveLog)
        liveInstruments.remove(devLog)

        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(liveLog)
        dispatchCommand(accessToken, ProbeAddress.LIVE_LOG_REMOTE, liveLog.location, debuggerCommand)

        val jvmCause = if (cause == null) null else LiveStackTrace.fromString(cause)
        val waitingHandler = waitingApply.remove(liveLog.id)
        if (waitingHandler != null) {
            if (cause?.startsWith("EventBusException") == true) {
                val ebException = EventBusUtil.fromEventBusException(cause)
                waitingHandler.handle(Future.failedFuture(ebException))
            } else {
                TODO("$cause")
            }
        } else {
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(
                        LiveInstrumentEventType.LOG_REMOVED,
                        //todo: could send whole log instead of just id
                        Json.encode(LiveLogRemoved(liveLog.id!!, occurredAt, jvmCause, liveLog))
                    )
                )
            )
        }

        if (jvmCause != null) {
            log.warn("Publish live log removed. Cause: {} - {}", jvmCause.exceptionType, jvmCause.message)
        } else {
            log.info("Published live log removed")
        }
    }

    private fun removeLiveMeter(
        selfId: String, accessToken: String?,
        occurredAt: Instant, meter: LiveMeter, cause: String?
    ) {
        log.debug("Removing live meter: ${meter.id}")
        val devMeter = DeveloperInstrument(selfId, accessToken, meter)
        liveInstruments.remove(devMeter)

        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(meter)
        dispatchCommand(accessToken, ProbeAddress.LIVE_METER_REMOTE, meter.location, debuggerCommand)

        val jvmCause = if (cause == null) null else LiveStackTrace.fromString(cause)
        val waitingHandler = waitingApply.remove(meter.id)
        if (waitingHandler != null) {
            if (cause?.startsWith("EventBusException") == true) {
                val ebException = EventBusUtil.fromEventBusException(cause)
                waitingHandler.handle(Future.failedFuture(ebException))
            } else {
                TODO("$cause")
            }
        } else {
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(
                        LiveInstrumentEventType.METER_REMOVED,
                        //todo: could send whole meter instead of just id
                        Json.encode(LiveMeterRemoved(meter.id!!, occurredAt, jvmCause))
                    )
                )
            )
        }

        if (jvmCause != null) {
            log.warn("Publish live meter removed. Cause: {}", jvmCause.message)
        } else {
            log.info("Published live meter removed")
        }
    }

    private fun removeLiveSpan(
        selfId: String, accessToken: String?,
        occurredAt: Instant, span: LiveSpan, cause: String?
    ) {
        log.debug("Removing live span: ${span.id}")
        val devMeter = DeveloperInstrument(selfId, accessToken, span)
        liveInstruments.remove(devMeter)

        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(span)
        dispatchCommand(accessToken, ProbeAddress.LIVE_SPAN_REMOTE, span.location, debuggerCommand)

        val jvmCause = if (cause == null) null else LiveStackTrace.fromString(cause)
        val waitingHandler = waitingApply.remove(span.id)
        if (waitingHandler != null) {
            if (cause?.startsWith("EventBusException") == true) {
                val ebException = EventBusUtil.fromEventBusException(cause)
                waitingHandler.handle(Future.failedFuture(ebException))
            } else {
                TODO("$cause")
            }
        } else {
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(
                        LiveInstrumentEventType.SPAN_REMOVED,
                        //todo: could send whole span instead of just id
                        Json.encode(LiveSpanRemoved(span.id!!, occurredAt, jvmCause))
                    )
                )
            )
        }

        if (jvmCause != null) {
            log.warn("Publish live span removed. Cause: {}", jvmCause.message)
        } else {
            log.info("Published live span removed")
        }
    }

    fun removeLiveInstrument(selfId: String, accessToken: String?, instrumentId: String): AsyncResult<LiveInstrument?> {
        if (log.isTraceEnabled) log.trace("Removing live instrument: $instrumentId")
        val instrumentRemoval = liveInstruments.find { it.instrument.id == instrumentId }
        return if (instrumentRemoval != null) {
            removeLiveInstrument(selfId, accessToken, instrumentRemoval)
        } else {
            Future.succeededFuture()
        }
    }

    fun removeLiveInstrument(
        selfId: String, accessToken: String?,
        instrumentRemoval: DeveloperInstrument
    ): AsyncResult<LiveInstrument?> {
        if (instrumentRemoval.instrument.id == null) {
            //unpublished instrument; just remove from platform
            liveInstruments.remove(instrumentRemoval)
            return Future.succeededFuture(instrumentRemoval.instrument)
        }

        //publish remove command to all probes
        when (instrumentRemoval.instrument) {
            is LiveBreakpoint -> removeLiveBreakpoint(
                selfId,
                accessToken,
                Clock.System.now(),
                instrumentRemoval.instrument,
                null
            )
            is LiveLog -> removeLiveLog(selfId, accessToken, Clock.System.now(), instrumentRemoval.instrument, null)
            is LiveMeter -> removeLiveMeter(selfId, accessToken, Clock.System.now(), instrumentRemoval.instrument, null)
            is LiveSpan -> removeLiveSpan(selfId, accessToken, Clock.System.now(), instrumentRemoval.instrument, null)
            else -> TODO()
        }

        return Future.succeededFuture(instrumentRemoval.instrument)
    }

    fun removeBreakpoints(
        selfId: String,
        accessToken: String?,
        location: LiveSourceLocation
    ): AsyncResult<List<LiveInstrument>> {
        log.debug("Removing live breakpoint(s): $location")
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLocation(location)

        val result = liveInstruments.filter { it.instrument.location == location && it.instrument is LiveBreakpoint }
        liveInstruments.removeAll(result.toSet())
        if (result.isEmpty()) {
            log.info("Could not find live breakpoint(s) at: $location")
        } else {
            dispatchCommand(accessToken, ProbeAddress.LIVE_BREAKPOINT_REMOTE, location, debuggerCommand)
            log.debug("Removed live breakpoint(s) at: $location")

            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(LiveInstrumentEventType.BREAKPOINT_REMOVED, Json.encode(result))
                )
            )
        }
        return Future.succeededFuture(result.map { it.instrument as LiveBreakpoint })
    }

    fun removeLogs(
        selfId: String,
        accessToken: String?,
        location: LiveSourceLocation
    ): AsyncResult<List<LiveInstrument>> {
        log.debug("Removing live log(s): $location")
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLocation(location)

        val result = liveInstruments.filter { it.instrument.location == location && it.instrument is LiveLog }
        liveInstruments.removeAll(result.toSet())
        if (result.isEmpty()) {
            log.info("Could not find live log(s) at: $location")
        } else {
            dispatchCommand(accessToken, ProbeAddress.LIVE_LOG_REMOTE, location, debuggerCommand)
            log.debug("Removed live log(s) at: $location")

            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(LiveInstrumentEventType.LOG_REMOVED, Json.encode(result))
                )
            )
        }
        return Future.succeededFuture(result.map { it.instrument as LiveLog })
    }

    fun removeMeters(
        selfId: String,
        accessToken: String?,
        location: LiveSourceLocation
    ): AsyncResult<List<LiveInstrument>> {
        log.debug("Removing live meter(s): $location")
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLocation(location)

        val result = liveInstruments.filter { it.instrument.location == location && it.instrument is LiveMeter }
        liveInstruments.removeAll(result.toSet())
        if (result.isEmpty()) {
            log.info("Could not find live meter(s) at: $location")
        } else {
            dispatchCommand(accessToken, ProbeAddress.LIVE_METER_REMOTE, location, debuggerCommand)
            log.debug("Removed live meter(s) at: $location")

            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(
                    LiveInstrumentEvent(LiveInstrumentEventType.METER_REMOVED, Json.encode(result))
                )
            )
        }
        return Future.succeededFuture(result.map { it.instrument as LiveMeter })
    }

    //todo: impl probe clear command
    fun clearAllLiveInstruments(selfId: String, accessToken: String): AsyncResult<Boolean> {
        val allLiveInstruments = getLiveInstruments()
        allLiveInstruments.forEach {
            removeLiveInstrument(selfId, accessToken, it.id!!)
        }
        return Future.succeededFuture(true)
    }

    //todo: impl probe clear command
    fun clearLiveInstruments(selfId: String, accessToken: String?, handler: Handler<AsyncResult<Boolean>>) {
        val devInstruments = liveInstruments.filter { it.selfId == selfId }
        devInstruments.forEach {
            removeLiveInstrument(selfId, accessToken, it.instrument.id!!)
        }
        handler.handle(Future.succeededFuture(true))
    }

    //todo: impl probe clear command
    fun clearLiveBreakpoints(selfId: String, accessToken: String?, handler: Handler<AsyncResult<Boolean>>) {
        val devBreakpoints = liveInstruments.filter { it.selfId == selfId && it.instrument is LiveBreakpoint }
        devBreakpoints.forEach {
            removeLiveInstrument(selfId, accessToken, it.instrument.id!!)
        }
        handler.handle(Future.succeededFuture(true))
    }

    fun clearLiveLogs(selfId: String, accessToken: String?, handler: Handler<AsyncResult<Boolean>>) {
        val devLogs = liveInstruments.filter { it.selfId == selfId && it.instrument is LiveLog }
        devLogs.forEach {
            removeLiveInstrument(selfId, accessToken, it.instrument.id!!)
        }
        handler.handle(Future.succeededFuture(true))
    }

    fun clearLiveMeters(selfId: String, accessToken: String?, handler: Handler<AsyncResult<Boolean>>) {
        val devMeters = liveInstruments.filter { it.selfId == selfId && it.instrument is LiveMeter }
        devMeters.forEach {
            removeLiveInstrument(selfId, accessToken, it.instrument.id!!)
        }
        handler.handle(Future.succeededFuture(true))
    }

    fun clearLiveSpans(selfId: String, accessToken: String?, handler: Handler<AsyncResult<Boolean>>) {
        val devSpans = liveInstruments.filter { it.selfId == selfId && it.instrument is LiveSpan }
        devSpans.forEach {
            removeLiveInstrument(selfId, accessToken, it.instrument.id!!)
        }
        handler.handle(Future.succeededFuture(true))
    }

    data class DeveloperInstrument(
        val selfId: String,
        val accessToken: String?,
        val instrument: LiveInstrument
    ) {
        //todo: verify selfId isn't needed in equals/hashcode
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is DeveloperInstrument) return false
            if (instrument != other.instrument) return false
            return true
        }

        override fun hashCode(): Int = Objects.hash(selfId, instrument)
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
                        exp =
                            "($idVariable.sum(['service', 'instance']).downsampling(SUM)).instance(['service'], ['instance'])"
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
                        exp =
                            "($idVariable.sum(['le', 'service', 'instance']).increase('PT5M').histogram().histogram_percentile([50,70,90,99])).instance(['service'], ['instance'])"
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
