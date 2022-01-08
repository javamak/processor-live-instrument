package spp.processor.live.impl.instrument

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.protobuf.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.get
import kotlinx.datetime.Instant
import org.apache.skywalking.apm.network.language.agent.v3.SegmentObject
import org.apache.skywalking.apm.network.language.agent.v3.SpanObject
import org.apache.skywalking.apm.network.logging.v3.LogData
import org.apache.skywalking.oap.log.analyzer.provider.log.listener.LogAnalysisListener
import org.apache.skywalking.oap.log.analyzer.provider.log.listener.LogAnalysisListenerFactory
import org.apache.skywalking.oap.server.analyzer.provider.AnalyzerModuleConfig
import org.apache.skywalking.oap.server.analyzer.provider.trace.parser.listener.*
import org.apache.skywalking.oap.server.library.module.ModuleManager
import org.slf4j.LoggerFactory
import spp.processor.InstrumentProcessor
import spp.protocol.artifact.exception.LiveStackTrace
import spp.protocol.artifact.exception.LiveStackTraceElement
import spp.protocol.artifact.exception.sourceAsLineNumber
import spp.protocol.instrument.LiveVariable
import spp.protocol.instrument.LiveVariableScope
import spp.protocol.instrument.breakpoint.event.LiveBreakpointHit
import spp.protocol.processor.ProcessorAddress
import spp.protocol.processor.ProcessorAddress.BREAKPOINT_HIT
import java.util.concurrent.TimeUnit

class LiveInstrumentAnalysis : AnalysisListenerFactory, LogAnalysisListenerFactory {

    companion object {
        private val log = LoggerFactory.getLogger(LiveInstrumentAnalysis::class.java)

        const val LOCAL_VARIABLE = "spp.local-variable:"
        const val GLOBAL_VARIABLE = "spp.global-variable:"
        const val STATIC_FIELD = "spp.static-field:"
        const val INSTANCE_FIELD = "spp.field:"
        const val STACK_TRACE = "spp.stack-trace:"
        const val BREAKPOINT = "spp.breakpoint:"

        private fun toLiveVariable(varName: String, scope: LiveVariableScope?, varData: JsonObject): LiveVariable {
            val liveClass = varData.getString("@class")
            val liveIdentity = varData.getString("@identity")

            val innerVars = mutableListOf<LiveVariable>()
            varData.fieldNames().forEach {
                if (!it.startsWith("@")) {
                    if (varData.get<Any>(it) is JsonObject) {
                        innerVars.add(toLiveVariable(it, null, varData.getJsonObject(it)))
                    } else {
                        innerVars.add(LiveVariable(it, varData[it]))
                    }
                }
            }
            return LiveVariable(varName, innerVars, scope = scope, liveClazz = liveClass, liveIdentity = liveIdentity)
        }

        fun transformRawBreakpointHit(bpData: JsonObject): LiveBreakpointHit {
            val varDatum = bpData.getJsonArray("variables")
            val variables = mutableListOf<LiveVariable>()
            var thisVar: LiveVariable? = null
            for (i in varDatum.list.indices) {
                val varData = varDatum.getJsonObject(i)
                val varName = varData.getJsonObject("data").fieldNames().first()
                val outerVal = JsonObject(varData.getJsonObject("data").getString(varName))
                val scope = LiveVariableScope.valueOf(varData.getString("scope"))

                val liveVar = if (outerVal.get<Any>(varName) is JsonObject) {
                    toLiveVariable(varName, scope, outerVal.getJsonObject(varName))
                } else {
                    LiveVariable(
                        varName, outerVal[varName],
                        scope = scope,
                        liveClazz = outerVal.getString("@class"),
                        liveIdentity = outerVal.getString("@identity")
                    )
                }
                variables.add(liveVar)

                if (liveVar.name == "this") {
                    thisVar = liveVar
                }
            }

            //put instance variables in "this"
            if (thisVar?.value is List<*>) {
                val thisVariables = thisVar.value as MutableList<LiveVariable>?
                variables.filter { it.scope == LiveVariableScope.INSTANCE_FIELD }.forEach { v ->
                    thisVariables?.removeIf { rem ->
                        if (rem.name == v.name) {
                            variables.removeIf { it.name == v.name }
                            true
                        } else {
                            false
                        }
                    }
                    thisVariables?.add(v)
                }
            }

            val stackTrace = LiveStackTrace.fromString(bpData.getString("stack_trace"))!!
            //correct unknown source
            if (stackTrace.first().sourceAsLineNumber() == null) {
                val language = stackTrace.elements[1].source.substringAfter(".").substringBefore(":")
                val actualSource = "${
                    bpData.getString("location_source").substringAfterLast(".")
                }.$language:${bpData.getInteger("location_line")}"
                val correctedElement = LiveStackTraceElement(stackTrace.first().method, actualSource)
                stackTrace.elements.removeAt(0)
                stackTrace.elements.add(0, correctedElement)
            }
            //add live variables
            stackTrace.first().variables.addAll(variables)

            return LiveBreakpointHit(
                bpData.getString("breakpoint_id"),
                bpData.getString("trace_id"),
                Instant.fromEpochMilliseconds(bpData.getLong("occurred_at")),
                bpData.getString("service_instance"),
                bpData.getString("service"),
                stackTrace
            )
        }
    }

    private var logPublishRateLimit = 1000
    private val logPublishCache = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build(object : CacheLoader<String, Long>() {
            override fun load(key: String): Long = -1
        })

    init {
        //todo: map of rate limit per log id
        InstrumentProcessor.vertx.eventBus().consumer<Int>(ProcessorAddress.SET_LOG_PUBLISH_RATE_LIMIT.address) {
            logPublishRateLimit = it.body()
        }
    }

    private val sppLogAnalyzer = object : LogAnalysisListener {
        override fun build() = Unit

        override fun parse(logData: LogData.Builder, p1: Message?): LogAnalysisListener {
            if (log.isTraceEnabled) log.trace("Parsing log data {}", logData)
            var logId: String? = null
            var logger: String? = null
            var thread: String? = null
            val arguments = JsonArray()
            logData.tags.dataList.forEach {
                when {
                    "log_id" == it.key -> logId = it.value
                    "logger" == it.key -> logger = it.value
                    "thread" == it.key -> thread = it.value
                    it.key.startsWith("argument.") -> arguments.add(it.value)
                }
            }
            if (logId == null) return this

            val logLastPublished = logPublishCache.get(logId!!)
            if (System.currentTimeMillis() - logLastPublished < logPublishRateLimit) {
                return this
            }

            val logHit = JsonObject()
                .put("logId", logId)
                .put("occurredAt", logData.timestamp)
                .put("serviceInstance", logData.serviceInstance)
                .put("service", logData.service)
                .put(
                    "logResult",
                    JsonObject()
                        .put("orderType", "NEWEST_LOGS")
                        .put("timestamp", logData.timestamp)
                        .put(
                            "logs", JsonArray().add(
                                JsonObject()
                                    .put("timestamp", logData.timestamp)
                                    .put("content", logData.body.text.text)
                                    .put("level", "Live")
                                    .put("logger", logger)
                                    .put("thread", thread)
                                    .put("arguments", arguments)
                            )
                        )
                        .put("total", -1)
                )
            InstrumentProcessor.vertx.eventBus().publish(ProcessorAddress.LOG_HIT.address, logHit)
            logPublishCache.put(logId!!, System.currentTimeMillis())
            return this
        }
    }

    override fun create() = sppLogAnalyzer

    private class Listener : LocalAnalysisListener, EntryAnalysisListener, ExitAnalysisListener {
        override fun build() = Unit
        override fun containsPoint(point: AnalysisListener.Point): Boolean =
            point == AnalysisListener.Point.Local || point == AnalysisListener.Point.Entry ||
                    point == AnalysisListener.Point.Exit

        override fun parseExit(span: SpanObject, segment: SegmentObject) = parseSpan(span, segment)
        override fun parseEntry(span: SpanObject, segment: SegmentObject) = parseSpan(span, segment)
        override fun parseLocal(span: SpanObject, segment: SegmentObject) = parseSpan(span, segment)

        fun parseSpan(span: SpanObject, segment: SegmentObject) {
            if (log.isTraceEnabled) log.trace("Parsing span {} of {}", span, segment)
            val locationSources = mutableMapOf<String, String>()
            val locationLines = mutableMapOf<String, Int>()
            val variables = mutableMapOf<String, MutableList<MutableMap<String, Any>>>()
            val stackTraces = mutableMapOf<String, String>()
            val breakpointIds = mutableListOf<String>()
            span.tagsList.forEach {
                when {
                    it.key.startsWith(LOCAL_VARIABLE) -> {
                        val parts = it.key.substring(LOCAL_VARIABLE.length).split(":")
                        val breakpointId = parts[0]
                        variables.putIfAbsent(breakpointId, mutableListOf())
                        variables[breakpointId]!!.add(
                            mutableMapOf(
                                "scope" to "LOCAL_VARIABLE",
                                "data" to mutableMapOf(parts[1] to it.value)
                            )
                        )
                    }
                    it.key.startsWith(GLOBAL_VARIABLE) -> {
                        val parts = it.key.substring(GLOBAL_VARIABLE.length).split(":")
                        val breakpointId = parts[0]
                        variables.putIfAbsent(breakpointId, mutableListOf())
                        variables[breakpointId]!!.add(
                            mutableMapOf(
                                "scope" to "GLOBAL_VARIABLE",
                                "data" to mutableMapOf(parts[1] to it.value)
                            )
                        )
                    }
                    it.key.startsWith(STATIC_FIELD) -> {
                        val parts = it.key.substring(STATIC_FIELD.length).split(":")
                        val breakpointId = parts[0]
                        variables.putIfAbsent(breakpointId, mutableListOf())
                        variables[breakpointId]!!.add(
                            mutableMapOf(
                                "scope" to "STATIC_FIELD",
                                "data" to mutableMapOf(parts[1] to it.value)
                            )
                        )
                    }
                    it.key.startsWith(INSTANCE_FIELD) -> {
                        val parts = it.key.substring(INSTANCE_FIELD.length).split(":")
                        val breakpointId = parts[0]
                        variables.putIfAbsent(breakpointId, mutableListOf())
                        variables[breakpointId]!!.add(
                            mutableMapOf(
                                "scope" to "INSTANCE_FIELD",
                                "data" to mutableMapOf(parts[1] to it.value)
                            )
                        )
                    }
                    it.key.startsWith(STACK_TRACE) -> {
                        stackTraces[it.key.substring(STACK_TRACE.length)] = it.value
                    }
                    it.key.startsWith(BREAKPOINT) -> {
                        val breakpointId = it.key.substring(BREAKPOINT.length)
                        breakpointIds.add(breakpointId)
                        JsonObject(it.value).apply {
                            locationSources[breakpointId] = getString("source")
                            locationLines[breakpointId] = getInteger("line")
                        }
                    }
                }
            }

            breakpointIds.forEach {
                val bpHitObj = mapOf(
                    "breakpoint_id" to it,
                    "trace_id" to segment.traceId,
                    "stack_trace" to stackTraces[it]!!,
                    "variables" to variables.getOrDefault(it, emptyList()),
                    "occurred_at" to span.startTime,
                    "service_instance" to segment.serviceInstance,
                    "service" to segment.service,
                    "location_source" to locationSources[it]!!,
                    "location_line" to locationLines[it]!!
                )
                InstrumentProcessor.vertx.eventBus().publish(
                    BREAKPOINT_HIT.address,
                    //todo: don't need to map twice
                    JsonObject.mapFrom(transformRawBreakpointHit(JsonObject.mapFrom(bpHitObj)))
                )
            }
        }
    }

    private val listener: AnalysisListener = Listener()
    override fun create(p0: ModuleManager, p1: AnalyzerModuleConfig): AnalysisListener = listener
}
