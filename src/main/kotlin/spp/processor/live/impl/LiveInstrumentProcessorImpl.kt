package spp.processor.live.impl

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.eventbus.ReplyFailure
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
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
import spp.protocol.instrument.breakpoint.event.LiveBreakpointRemoved
import spp.protocol.instrument.log.LiveLog
import spp.protocol.instrument.log.event.LiveLogRemoved
import spp.protocol.instrument.meter.LiveMeter
import spp.protocol.instrument.meter.MeterType
import spp.protocol.instrument.meter.event.LiveMeterRemoved
import spp.protocol.instrument.span.LiveSpan
import spp.protocol.instrument.span.event.LiveSpanRemoved
import spp.protocol.platform.PlatformAddress
import spp.protocol.platform.client.ActiveProbe
import spp.protocol.probe.ProbeAddress
import spp.protocol.probe.ProbeAddress.*
import spp.protocol.probe.command.LiveInstrumentCommand
import spp.protocol.probe.command.LiveInstrumentContext
import spp.protocol.service.error.LiveInstrumentException
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
        FeedbackProcessor.module!!.find(StorageModule.NAME).provider().apply {
            metadata = getService(IMetadataQueryDAO::class.java)
        }
        FeedbackProcessor.module!!.find(CoreModule.NAME).provider().apply {
            metricsQueryService = getService(MetricsQueryService::class.java)
            meterSystem = getService(MeterSystem::class.java)
        }
        FeedbackProcessor.module!!.find(AnalyzerModule.NAME).provider().apply {
            meterProcessService = getService(IMeterProcessService::class.java) as MeterProcessService
        }

        vertx.setPeriodic(TimeUnit.SECONDS.toMillis(1)) {
            if (liveInstruments.isNotEmpty()) {
                liveInstruments.forEach {
                    if (it.instrument.pending
                        && it.instrument.expiresAt != null
                        && it.instrument.expiresAt!! <= System.currentTimeMillis()
                    ) {
                        removeLiveInstrument(it.developerAuth, it)
                    }
                }
            }
        }

        //send active instruments on probe connection
        vertx.eventBus().consumer<JsonObject>("local." + REMOTE_REGISTERED.address) {
            //todo: impl batch instrument add
            //todo: more efficient to just send batch add to specific probe instead of publish to all per connection
            //todo: probably need to redo pending boolean. it doesn't make sense here since pending just means
            // it has been applied to any instrument at all at any point
            val remote = it.body().getString("address").substringBefore(":")
            if (remote == LIVE_BREAKPOINT_REMOTE.address) {
                log.debug("Live breakpoint remote registered. Sending active live breakpoints")
                liveInstruments.filter { it.instrument is LiveBreakpoint }.forEach {
                    addLiveInstrument(it.developerAuth, it.instrument as LiveBreakpoint, false)
                }
            }
            if (remote == LIVE_LOG_REMOTE.address) {
                log.debug("Live log remote registered. Sending active live logs")
                liveInstruments.filter { it.instrument is LiveLog }.forEach {
                    addLiveInstrument(it.developerAuth, it.instrument as LiveLog, false)
                }
            }
            if (remote == LIVE_METER_REMOTE.address) {
                log.debug("Live meter remote registered. Sending active live meters")
                liveInstruments.filter { it.instrument is LiveMeter }.forEach {
                    addLiveInstrument(it.developerAuth, it.instrument as LiveMeter, false)
                }
            }
            if (remote == LIVE_SPAN_REMOTE.address) {
                log.debug("Live span remote registered. Sending active live spans")
                liveInstruments.filter { it.instrument is LiveSpan }.forEach {
                    addLiveInstrument(it.developerAuth, it.instrument as LiveSpan, false)
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
        val devAuth = DeveloperAuth.from(handler)
        addLiveInstrument(devAuth, instrument, handler)
    }

    private fun addLiveInstrument(
        devAuth: DeveloperAuth,
        instrument: LiveInstrument,
        handler: Handler<AsyncResult<LiveInstrument>>
    ) {
        log.info(
            "Received add live instrument request. Developer: {} - Location: {}",
            devAuth, instrument.location.let { it.source + ":" + it.line }
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
                                put("created_by", devAuth.selfId)
                                put("hit_count", AtomicInteger())
                            }
                        )

                        if (pendingBp.applyImmediately) {
                            addApplyImmediatelyHandler(pendingBp.id!!, handler)
                            addLiveInstrument(devAuth, pendingBp)
                        } else {
                            handler.handle(addLiveInstrument(devAuth, pendingBp))
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
                                put("created_by", devAuth.selfId)
                                put("hit_count", AtomicInteger())
                            }
                        )

                        if (pendingLog.applyImmediately) {
                            addApplyImmediatelyHandler(pendingLog.id!!, handler)
                            addLiveInstrument(devAuth, pendingLog)
                        } else {
                            handler.handle(addLiveInstrument(devAuth, pendingLog))
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
                                put("created_by", devAuth.selfId)
                            }
                        )

                        setupLiveMeter(pendingMeter)
                        if (pendingMeter.applyImmediately) {
                            addApplyImmediatelyHandler(pendingMeter.id!!, handler)
                            addLiveInstrument(devAuth, pendingMeter)
                        } else {
                            handler.handle(addLiveInstrument(devAuth, pendingMeter))
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
                                put("created_by", devAuth.selfId)
                            }
                        )

                        if (pendingSpan.applyImmediately) {
                            addApplyImmediatelyHandler(pendingSpan.id!!, handler)
                            addLiveInstrument(devAuth, pendingSpan)
                        } else {
                            handler.handle(addLiveInstrument(devAuth, pendingSpan))
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
        val devAuth = DeveloperAuth.from(handler)
        log.info(
            "Received add live instrument batch request. Developer: {} - Location(s): {}",
            devAuth, batch.instruments.map { it.location.let { it.source + ":" + it.line } }
        )

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                val results = mutableListOf<LiveInstrument>()
                batch.instruments.forEach {
                    val promise = Promise.promise<LiveInstrument>()
                    addLiveInstrument(devAuth, it, promise)
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
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received get live instruments request. Developer: {}", devAuth)

        handler.handle(Future.succeededFuture(getLiveInstruments()))
    }

    override fun removeLiveInstrument(id: String, handler: Handler<AsyncResult<LiveInstrument?>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received remove live instrument request. Developer: {} - Id: {}", devAuth, id)

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                handler.handle(removeLiveInstrument(devAuth, id))
            } catch (throwable: Throwable) {
                log.warn("Remove live instrument failed", throwable)
                handler.handle(Future.failedFuture(throwable))
            }
        }
    }

    override fun removeLiveInstruments(
        location: LiveSourceLocation, handler: Handler<AsyncResult<List<LiveInstrument>>>
    ) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received remove live instruments request. Developer: {} - Location: {}", devAuth, location)

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                val breakpointsResult = removeInstruments(devAuth, location, LiveInstrumentType.BREAKPOINT)
                val logsResult = removeInstruments(devAuth, location, LiveInstrumentType.LOG)
                val metersResult = removeInstruments(devAuth, location, LiveInstrumentType.METER)
                val spansResult = removeInstruments(devAuth, location, LiveInstrumentType.SPAN)

                when {
                    breakpointsResult.failed() -> handler.handle(Future.failedFuture(breakpointsResult.cause()))
                    logsResult.failed() -> handler.handle(Future.failedFuture(logsResult.cause()))
                    metersResult.failed() -> handler.handle(Future.failedFuture(metersResult.cause()))
                    spansResult.failed() -> handler.handle(Future.failedFuture(spansResult.cause()))
                    else -> handler.handle(
                        Future.succeededFuture(
                            breakpointsResult.result() + logsResult.result()
                                    + metersResult.result() + spansResult.result()
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
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received get live instrument by id request. Developer: {} - Id: {}", devAuth, id)

        handler.handle(Future.succeededFuture(getLiveInstrumentById(id)))
    }

    override fun getLiveInstrumentsByIds(ids: List<String>, handler: Handler<AsyncResult<List<LiveInstrument>>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received get live instruments by ids request. Developer: {} - Ids: {}", devAuth, ids)

        handler.handle(Future.succeededFuture(getLiveInstrumentsByIds(ids)))
    }

    override fun getLiveBreakpoints(handler: Handler<AsyncResult<List<LiveBreakpoint>>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received get live breakpoints request. Developer: {}", devAuth)

        handler.handle(Future.succeededFuture(getActiveLiveBreakpoints()))
    }

    override fun getLiveLogs(handler: Handler<AsyncResult<List<LiveLog>>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received get live logs request. Developer: {}", devAuth)

        handler.handle(Future.succeededFuture(getActiveLiveLogs()))
    }

    override fun getLiveMeters(handler: Handler<AsyncResult<List<LiveMeter>>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received get live meters request. Developer: {}", devAuth)

        handler.handle(Future.succeededFuture(getActiveLiveMeters()))
    }

    override fun getLiveSpans(handler: Handler<AsyncResult<List<LiveSpan>>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received get live spans request. Developer: {}", devAuth)

        handler.handle(Future.succeededFuture(getActiveLiveSpans()))
    }

    override fun clearAllLiveInstruments(handler: Handler<AsyncResult<Boolean>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received clear live instruments request. Developer: {}", devAuth)

        handler.handle(clearAllLiveInstruments(devAuth))
    }

    override fun clearLiveInstruments(handler: Handler<AsyncResult<Boolean>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received clear live instruments request. Developer: {}", devAuth.selfId)

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                clearLiveInstruments(devAuth, null, handler)
            } catch (throwable: Throwable) {
                log.warn("Clear live instruments failed", throwable)
                handler.handle(Future.failedFuture(throwable))
            }
        }
    }

    override fun clearLiveBreakpoints(handler: Handler<AsyncResult<Boolean>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received clear live breakpoints request. Developer: {}", devAuth)

        clearLiveInstruments(devAuth, LiveInstrumentType.BREAKPOINT, handler)
    }

    override fun clearLiveLogs(handler: Handler<AsyncResult<Boolean>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received clear live logs request. Developer: {}", devAuth)

        clearLiveInstruments(devAuth, LiveInstrumentType.LOG, handler)
    }

    override fun clearLiveMeters(handler: Handler<AsyncResult<Boolean>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received clear live meters request. Developer: {}", devAuth)

        clearLiveInstruments(devAuth, LiveInstrumentType.METER, handler)
    }

    override fun clearLiveSpans(handler: Handler<AsyncResult<Boolean>>) {
        val devAuth = DeveloperAuth.from(handler)
        log.info("Received clear live spans request. Developer: {}", devAuth)

        clearLiveInstruments(devAuth, LiveInstrumentType.SPAN, handler)
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
        vertx.eventBus().localConsumer<JsonObject>("local." + PlatformAddress.LIVE_BREAKPOINT_APPLIED.address) {
            handleLiveInstrumentApplied(LiveBreakpoint::class.java, it)
        }
        vertx.eventBus().localConsumer<JsonObject>("local." + PlatformAddress.LIVE_BREAKPOINT_REMOVED.address) {
            handleBreakpointRemoved(it)
        }
    }

    private fun handleBreakpointRemoved(it: Message<JsonObject>) {
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
            removeLiveInstrument(
                instrumentRemoval.developerAuth,
                Instant.fromEpochMilliseconds(it.body().getLong("occurredAt")),
                instrumentRemoval.instrument as LiveBreakpoint,
                it.body().getString("cause")
            )
        }
    }

    private fun listenForLiveLogs() {
        vertx.eventBus().localConsumer<JsonObject>("local." + PlatformAddress.LIVE_LOG_APPLIED.address) {
            handleLiveInstrumentApplied(LiveLog::class.java, it)
        }
        vertx.eventBus().localConsumer<JsonObject>("local." + PlatformAddress.LIVE_LOG_REMOVED.address) {
            handleLogRemoved(it)
        }
    }

    private fun handleLogRemoved(it: Message<JsonObject>) {
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
            removeLiveInstrument(
                instrumentRemoval.developerAuth,
                Instant.fromEpochMilliseconds(it.body().getLong("occurredAt")),
                instrumentRemoval.instrument as LiveLog,
                it.body().getString("cause")
            )
        }
    }

    private fun listenForLiveMeters() {
        vertx.eventBus().localConsumer<JsonObject>("local." + PlatformAddress.LIVE_METER_APPLIED.address) {
            handleLiveInstrumentApplied(LiveMeter::class.java, it)
        }
        vertx.eventBus().localConsumer<JsonObject>("local." + PlatformAddress.LIVE_METER_REMOVED.address) {
            handleMeterRemoved(it)
        }
    }

    private fun handleMeterRemoved(it: Message<JsonObject>) {
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
            removeLiveInstrument(
                instrumentRemoval.developerAuth,
                Instant.fromEpochMilliseconds(it.body().getLong("occurredAt")),
                instrumentRemoval.instrument as LiveMeter,
                it.body().getString("cause")
            )
        }
    }

    private fun listenForLiveSpans() {
        vertx.eventBus().localConsumer<JsonObject>("local." + PlatformAddress.LIVE_SPAN_APPLIED.address) {
            handleLiveInstrumentApplied(LiveSpan::class.java, it)
        }
        vertx.eventBus().localConsumer<JsonObject>("local." + PlatformAddress.LIVE_SPAN_REMOVED.address) {
            handleSpanRemoved(it)
        }
    }

    private fun handleSpanRemoved(it: Message<JsonObject>) {
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
            removeLiveInstrument(
                instrumentRemoval.developerAuth,
                Instant.fromEpochMilliseconds(it.body().getLong("occurredAt")),
                instrumentRemoval.instrument as LiveSpan,
                it.body().getString("cause")
            )
        }
    }

    private fun <T : LiveInstrument> handleLiveInstrumentApplied(clazz: Class<T>, it: Message<JsonObject>) {
        val liveInstrument = Json.decodeValue(it.body().toString(), clazz)
        liveInstruments.forEach {
            if (it.instrument.id == liveInstrument.id) {
                log.info("Live instrument applied. Id: {}", it.instrument.id)
                val eventType: LiveInstrumentEventType
                val appliedInstrument: LiveInstrument
                when (clazz) {
                    LiveBreakpoint::class.java -> {
                        eventType = LiveInstrumentEventType.BREAKPOINT_APPLIED
                        appliedInstrument = (it.instrument as LiveBreakpoint).copy(
                            applied = true,
                            pending = false
                        )
                    }
                    LiveLog::class.java -> {
                        eventType = LiveInstrumentEventType.LOG_APPLIED
                        appliedInstrument = (it.instrument as LiveLog).copy(
                            applied = true,
                            pending = false
                        )
                    }
                    LiveMeter::class.java -> {
                        eventType = LiveInstrumentEventType.METER_APPLIED
                        appliedInstrument = (it.instrument as LiveMeter).copy(
                            applied = true,
                            pending = false
                        )
                    }
                    LiveSpan::class.java -> {
                        eventType = LiveInstrumentEventType.SPAN_APPLIED
                        appliedInstrument = (it.instrument as LiveSpan).copy(
                            applied = true,
                            pending = false
                        )
                    }
                    else -> throw IllegalArgumentException("Unknown live instrument type")
                }
                (appliedInstrument.meta as MutableMap<String, Any>)["applied_at"] = "${System.currentTimeMillis()}"

                val devInstrument = DeveloperInstrument(it.developerAuth, appliedInstrument)
                liveInstruments.remove(it)
                liveInstruments.add(devInstrument)

                waitingApply.remove(appliedInstrument.id)?.handle(Future.succeededFuture(devInstrument))

                vertx.eventBus().publish(
                    SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                    JsonObject.mapFrom(LiveInstrumentEvent(eventType, Json.encode(appliedInstrument)))
                )
                if (log.isTraceEnabled) log.trace("Published live instrument applied")
                return@forEach
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

    fun addLiveInstrument(
        devAuth: DeveloperAuth, liveInstrument: LiveInstrument, alertSubscribers: Boolean = true
    ): AsyncResult<LiveInstrument> {
        log.debug("Adding live instrument: $liveInstrument")
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.ADD_LIVE_INSTRUMENT, LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(liveInstrument)

        val probeAddress = when (liveInstrument.type) {
            LiveInstrumentType.BREAKPOINT -> LIVE_BREAKPOINT_REMOTE
            LiveInstrumentType.LOG -> LIVE_LOG_REMOTE
            LiveInstrumentType.METER -> LIVE_METER_REMOTE
            LiveInstrumentType.SPAN -> LIVE_SPAN_REMOTE
        }
        val devInstrument = DeveloperInstrument(devAuth, liveInstrument)
        liveInstruments.add(devInstrument)
        try {
            dispatchCommand(devAuth.accessToken, probeAddress, liveInstrument.location, debuggerCommand)
        } catch (ex: ReplyException) {
            return if (ex.failureType() == ReplyFailure.NO_HANDLERS) {
                if (liveInstrument.applyImmediately) {
                    liveInstruments.remove(devInstrument)
                    log.warn("Live instrument failed due to missing remote(s)")
                    Future.failedFuture(MissingRemoteException(probeAddress.address).toEventBusException())
                } else {
                    log.info("Live instrument pending application on probe connection")
                    Future.succeededFuture(liveInstrument)
                }
            } else {
                liveInstruments.remove(devInstrument)
                log.warn("Failed to add live instrument: Reason: {}", ex.message)
                Future.failedFuture(ex)
            }
        }

        if (alertSubscribers) {
            val eventType = when (liveInstrument.type) {
                LiveInstrumentType.BREAKPOINT -> LiveInstrumentEventType.BREAKPOINT_ADDED
                LiveInstrumentType.LOG -> LiveInstrumentEventType.LOG_ADDED
                LiveInstrumentType.METER -> LiveInstrumentEventType.METER_ADDED
                LiveInstrumentType.SPAN -> LiveInstrumentEventType.SPAN_ADDED
            }
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(LiveInstrumentEvent(eventType, Json.encode(liveInstrument)))
            )
        }
        return Future.succeededFuture(liveInstrument)
    }

    private fun dispatchCommand(
        accessToken: String?,
        address: ProbeAddress,
        location: LiveSourceLocation,
        debuggerCommand: LiveInstrumentCommand
    ) = GlobalScope.launch(vertx.dispatcher()) {
        val promise = Promise.promise<JsonArray>()
        InstrumentProcessor.requestEvent<JsonArray>(
            SourceMarkerServices.Utilize.LIVE_SERVICE, JsonObject(),
            JsonObject().apply { accessToken?.let { put("auth-token", accessToken) } }.put("action", "getActiveProbes")
        ) {
            if (it.succeeded()) {
                promise.complete(it.result())
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

    private fun removeLiveInstrument(
        devAuth: DeveloperAuth, occurredAt: Instant, liveInstrument: LiveInstrument, cause: String?
    ) {
        log.debug("Removing live instrument: ${liveInstrument.id}")
        val devInstrument = DeveloperInstrument(devAuth, liveInstrument)
        liveInstruments.remove(devInstrument)

        val probeAddress = when (liveInstrument.type) {
            LiveInstrumentType.BREAKPOINT -> LIVE_BREAKPOINT_REMOTE
            LiveInstrumentType.LOG -> LIVE_LOG_REMOTE
            LiveInstrumentType.METER -> LIVE_METER_REMOTE
            LiveInstrumentType.SPAN -> LIVE_SPAN_REMOTE
        }
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT, LiveInstrumentContext()
        )
        debuggerCommand.context.addLiveInstrument(liveInstrument)
        dispatchCommand(devAuth.accessToken, probeAddress, liveInstrument.location, debuggerCommand)

        val jvmCause = if (cause == null) null else LiveStackTrace.fromString(cause)
        val waitingHandler = waitingApply.remove(liveInstrument.id)
        if (waitingHandler != null) {
            if (cause?.startsWith("EventBusException") == true) {
                val ebException = fromEventBusException(cause)
                waitingHandler.handle(Future.failedFuture(ebException))
            } else {
                TODO("$cause")
            }
        } else {
            val eventType = when (liveInstrument.type) {
                LiveInstrumentType.BREAKPOINT -> LiveInstrumentEventType.BREAKPOINT_REMOVED
                LiveInstrumentType.LOG -> LiveInstrumentEventType.LOG_REMOVED
                LiveInstrumentType.METER -> LiveInstrumentEventType.METER_REMOVED
                LiveInstrumentType.SPAN -> LiveInstrumentEventType.SPAN_REMOVED
            }
            val eventData = when (liveInstrument.type) {
                LiveInstrumentType.BREAKPOINT -> {
                    Json.encode(LiveBreakpointRemoved(liveInstrument.id!!, occurredAt, jvmCause))
                }
                LiveInstrumentType.LOG -> {
                    Json.encode(LiveLogRemoved(liveInstrument.id!!, occurredAt, jvmCause, liveInstrument as LiveLog))
                }
                LiveInstrumentType.METER -> {
                    Json.encode(LiveMeterRemoved(liveInstrument.id!!, occurredAt, jvmCause))
                }
                LiveInstrumentType.SPAN -> {
                    Json.encode(LiveSpanRemoved(liveInstrument.id!!, occurredAt, jvmCause))
                }
            }
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(LiveInstrumentEvent(eventType, eventData))
            )
        }

        if (jvmCause != null) {
            log.warn("Publish live instrument removed. Cause: {} - {}", jvmCause.exceptionType, jvmCause.message)
        } else {
            log.info("Published live instrument removed")
        }
    }

    fun removeLiveInstrument(developerAuth: DeveloperAuth, instrumentId: String): AsyncResult<LiveInstrument?> {
        if (log.isTraceEnabled) log.trace("Removing live instrument: $instrumentId")
        val instrumentRemoval = liveInstruments.find { it.instrument.id == instrumentId }
        return if (instrumentRemoval != null) {
            removeLiveInstrument(developerAuth, instrumentRemoval)
        } else {
            Future.succeededFuture()
        }
    }

    fun removeLiveInstrument(
        devAuth: DeveloperAuth, instrumentRemoval: DeveloperInstrument
    ): AsyncResult<LiveInstrument?> {
        if (instrumentRemoval.instrument.id == null) {
            //unpublished instrument; just remove from platform
            liveInstruments.remove(instrumentRemoval)
            return Future.succeededFuture(instrumentRemoval.instrument)
        }

        //publish remove command to all probes
        removeLiveInstrument(devAuth, Clock.System.now(), instrumentRemoval.instrument, null)
        return Future.succeededFuture(instrumentRemoval.instrument)
    }

    fun removeInstruments(
        devAuth: DeveloperAuth, location: LiveSourceLocation, instrumentType: LiveInstrumentType
    ): AsyncResult<List<LiveInstrument>> {
        log.debug("Removing live instrument(s): $location")
        val debuggerCommand = LiveInstrumentCommand(
            LiveInstrumentCommand.CommandType.REMOVE_LIVE_INSTRUMENT,
            LiveInstrumentContext()
        )
        debuggerCommand.context.addLocation(location)

        val result = liveInstruments.filter {
            it.instrument.location == location && it.instrument.type == instrumentType
        }
        liveInstruments.removeAll(result.toSet())
        if (result.isEmpty()) {
            log.info("Could not find live instrument(s) at: $location")
        } else {
            val probeAddress = when (instrumentType) {
                LiveInstrumentType.BREAKPOINT -> LIVE_BREAKPOINT_REMOTE
                LiveInstrumentType.LOG -> LIVE_LOG_REMOTE
                LiveInstrumentType.METER -> LIVE_METER_REMOTE
                LiveInstrumentType.SPAN -> LIVE_SPAN_REMOTE
            }
            dispatchCommand(devAuth.accessToken, probeAddress, location, debuggerCommand)
            log.debug("Removed live instrument(s) at: $location")

            val eventType = when (instrumentType) {
                LiveInstrumentType.BREAKPOINT -> LiveInstrumentEventType.BREAKPOINT_REMOVED
                LiveInstrumentType.LOG -> LiveInstrumentEventType.LOG_REMOVED
                LiveInstrumentType.METER -> LiveInstrumentEventType.METER_REMOVED
                LiveInstrumentType.SPAN -> LiveInstrumentEventType.SPAN_REMOVED
            }
            vertx.eventBus().publish(
                SourceMarkerServices.Provide.LIVE_INSTRUMENT_SUBSCRIBER,
                JsonObject.mapFrom(LiveInstrumentEvent(eventType, Json.encode(result)))
            )
        }
        return Future.succeededFuture(result.filter { it.instrument.type == instrumentType }.map { it.instrument })
    }

    //todo: impl probe clear command
    private fun clearAllLiveInstruments(devAuth: DeveloperAuth): AsyncResult<Boolean> {
        val allLiveInstruments = getLiveInstruments()
        allLiveInstruments.forEach {
            removeLiveInstrument(devAuth, it.id!!)
        }
        return Future.succeededFuture(true)
    }

    //todo: impl probe clear command
    private fun clearLiveInstruments(
        devAuth: DeveloperAuth, type: LiveInstrumentType?, handler: Handler<AsyncResult<Boolean>>
    ) {
        val devInstruments = liveInstruments.filter {
            it.developerAuth == devAuth && (type == null || it.instrument.type == type)
        }
        devInstruments.forEach {
            removeLiveInstrument(devAuth, it.instrument.id!!)
        }
        handler.handle(Future.succeededFuture(true))
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
        log.debug("Getting live meter metrics. Metric id: {}", liveMeter.toMetricId())
        val services = metadata.getAllServices(liveMeter.location.service ?: "")
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
                val serviceInstance = liveMeter.location.serviceInstance
                if (serviceInstance != null && serviceInstance != instance.name) {
                    return@forEach
                }

                val condition = MetricsCondition().apply {
                    name = liveMeter.toMetricId()
                    entity = Entity().apply {
                        setScope(Scope.ServiceInstance)
                        setNormal(true)
                        setServiceName(service.name)
                        setServiceInstanceName(instance.name)
                    }
                }
                if (liveMeter.toMetricId().contains("histogram")) {
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

    private fun fromEventBusException(exception: String): Exception {
        return if (exception.startsWith("EventBusException")) {
            var exceptionType = exception.substringAfter("EventBusException:")
            exceptionType = exceptionType.substringBefore("[")
            var exceptionParams = exception.substringAfter("[")
            exceptionParams = exceptionParams.substringBefore("]")
            val exceptionMessage = exception.substringAfter("]: ").trim { it <= ' ' }
            if (LiveInstrumentException::class.java.simpleName == exceptionType) {
                LiveInstrumentException(
                    LiveInstrumentException.ErrorType.valueOf(exceptionParams),
                    exceptionMessage
                ).toEventBusException()
            } else {
                throw UnsupportedOperationException(exceptionType)
            }
        } else {
            throw IllegalArgumentException(exception)
        }
    }
}
