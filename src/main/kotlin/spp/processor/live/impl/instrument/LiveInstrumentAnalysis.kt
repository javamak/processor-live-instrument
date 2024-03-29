/*
 * Source++, the open-source live coding platform.
 * Copyright (C) 2022 CodeBrig, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package spp.processor.live.impl.instrument

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.protobuf.Message
import io.vertx.core.json.Json
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
import org.joor.Reflect
import org.slf4j.LoggerFactory
import spp.processor.InstrumentProcessor
import spp.processor.InstrumentProcessor.liveInstrumentProcessor
import spp.processor.common.FeedbackProcessor.Companion.vertx
import spp.protocol.SourceServices.Provide.toLiveInstrumentSubscriberAddress
import spp.protocol.artifact.exception.LiveStackTrace
import spp.protocol.artifact.exception.LiveStackTraceElement
import spp.protocol.artifact.exception.sourceAsLineNumber
import spp.protocol.artifact.log.Log
import spp.protocol.artifact.log.LogOrderType
import spp.protocol.artifact.log.LogResult
import spp.protocol.instrument.event.LiveBreakpointHit
import spp.protocol.instrument.event.LiveInstrumentEvent
import spp.protocol.instrument.event.LiveInstrumentEventType
import spp.protocol.instrument.event.LiveLogHit
import spp.protocol.instrument.variable.LiveVariable
import spp.protocol.instrument.variable.LiveVariableScope
import spp.protocol.platform.ProcessorAddress
import java.time.*
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.reflect.jvm.jvmName

class LiveInstrumentAnalysis : AnalysisListenerFactory, LogAnalysisListenerFactory {

    companion object {
        private val log = LoggerFactory.getLogger(LiveInstrumentAnalysis::class.java)

        const val LOCAL_VARIABLE = "spp.local-variable:"
        const val GLOBAL_VARIABLE = "spp.global-variable:"
        const val STATIC_FIELD = "spp.static-field:"
        const val INSTANCE_FIELD = "spp.field:"
        const val STACK_TRACE = "spp.stack-trace:"
        const val BREAKPOINT = "spp.breakpoint:"

        val supportedClasses = mapOf<String, () -> Reflect>(
            Date::class.jvmName to { Reflect.on(Date()) },
            Duration::class.jvmName to { Reflect.on(Duration.ofSeconds(1)) },
            java.time.Instant::class.jvmName to { Reflect.on(java.time.Instant.now()) },
            LocalDate::class.jvmName to { Reflect.on(LocalDate.now()) },
            LocalTime::class.jvmName to { Reflect.on(LocalTime.now()) },
            LocalDateTime::class.jvmName to { Reflect.on(LocalDateTime.now()) },
            OffsetDateTime::class.jvmName to { Reflect.on(OffsetDateTime.now()) },
            OffsetTime::class.jvmName to { Reflect.on(OffsetTime.now()) },
            ZonedDateTime::class.jvmName to { Reflect.on(ZonedDateTime.now()) },
            ZoneOffset::class.jvmName to { Reflect.on(ZoneOffset.ofTotalSeconds(0)) },
            "java.time.ZoneRegion" to { Reflect.onClass("java.time.ZoneRegion").call("ofId", "GMT", false) }
        )

        private fun toLiveVariable(
            varName: String,
            scope: LiveVariableScope?,
            varData: JsonObject
        ): Pair<LiveVariable, Reflect?> {
            val liveClass = varData.getString("@class")
            val liveIdentity = varData.getString("@identity")
            val innerVars = mutableListOf<LiveVariable>()
            val obj = supportedClasses[liveClass]?.invoke()
            varData.fieldNames().forEach {
                if (!it.startsWith("@")) {
                    if (varData.get<Any>(it) is JsonObject) {
                        val varPair = toLiveVariable(it, null, varData.getJsonObject(it))
                        innerVars.add(varPair.first)
                        obj?.let { _ -> obj.set(it, varPair.second) }
                    } else {
                        innerVars.add(LiveVariable(it, varData[it]))
                        obj?.let { _ -> obj.set(it, asSmallestObject(varData[it])) }
                    }
                }
            }

            return Pair(
                LiveVariable(
                    varName, innerVars, scope = scope, liveClazz = liveClass,
                    liveIdentity = liveIdentity, presentation = obj?.toString()
                ), obj
            )
        }

        private fun asSmallestObject(value: Any?): Any? {
            if (value is Number) {
                when (value.toLong()) {
                    in Byte.MIN_VALUE..Byte.MAX_VALUE -> return value.toByte()
                    in Int.MIN_VALUE..Int.MAX_VALUE -> return value.toInt()
                    in Long.MIN_VALUE..Long.MAX_VALUE -> return value.toLong()
                }
            }
            return value
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
                    toLiveVariable(varName, scope, outerVal.getJsonObject(varName)).first
                } else {
                    val liveClazz = outerVal.getString("@class")
                    val presentation =
                        if (liveClazz !in supportedClasses) null else outerVal[varName]
                    LiveVariable(
                        varName, outerVal[varName],
                        scope = scope,
                        liveClazz = liveClazz,
                        liveIdentity = outerVal.getString("@identity"),
                        presentation = presentation
                    )
                }
                //liveVar = liveVar.copy(presentation = ReflectiveValueFormatter.format(liveVar.liveClazz, liveVar))
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
        vertx.eventBus().consumer<Int>(ProcessorAddress.SET_LOG_PUBLISH_RATE_LIMIT) {
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
            val arguments = mutableListOf<String>()
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

            handleLogHit(
                LiveLogHit(
                    logId!!,
                    Instant.fromEpochMilliseconds(logData.timestamp),
                    logData.serviceInstance,
                    logData.service,
                    LogResult(
                        orderType = LogOrderType.NEWEST_LOGS,
                        timestamp = Instant.fromEpochMilliseconds(logData.timestamp),
                        logs = listOf(
                            Log(
                                timestamp = Instant.fromEpochMilliseconds(logData.timestamp),
                                content = logData.body.text.text,
                                level = "Live",
                                logger = logger,
                                thread = thread,
                                arguments = arguments
                            )
                        ),
                        total = -1
                    )
                )
            )
            logPublishCache.put(logId!!, System.currentTimeMillis())
            return this
        }

        private fun handleLogHit(hit: LiveLogHit) {
            if (log.isTraceEnabled) log.trace("Live log hit: {}", hit)
            val liveInstrument = liveInstrumentProcessor._getDeveloperInstrumentById(hit.logId)
            if (liveInstrument != null) {
                val instrumentMeta = liveInstrument.instrument.meta as MutableMap<String, Any>
                if ((instrumentMeta["hit_count"] as AtomicInteger?)?.incrementAndGet() == 1) {
                    instrumentMeta["first_hit_at"] = System.currentTimeMillis().toString()
                }
                instrumentMeta["last_hit_at"] = System.currentTimeMillis().toString()
            }

            val devInstrument = liveInstrument ?: liveInstrumentProcessor.getCachedDeveloperInstrument(hit.logId)
            InstrumentProcessor.publishEvent(
                toLiveInstrumentSubscriberAddress(devInstrument.developerAuth.selfId),
                JsonObject.mapFrom(
                    LiveInstrumentEvent(LiveInstrumentEventType.LOG_HIT, JsonObject.mapFrom(hit).toString())
                )
            )
            if (log.isTraceEnabled) log.trace("Published live log hit")
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
                handleBreakpointHit(transformRawBreakpointHit(JsonObject.mapFrom(bpHitObj)))
            }
        }

        private fun handleBreakpointHit(hit: LiveBreakpointHit) {
            if (log.isTraceEnabled) log.trace("Live breakpoint hit: {}", hit)
            val liveInstrument = liveInstrumentProcessor._getDeveloperInstrumentById(hit.breakpointId)
            if (liveInstrument != null) {
                val instrumentMeta = liveInstrument.instrument.meta as MutableMap<String, Any>
                if ((instrumentMeta["hit_count"] as AtomicInteger?)?.incrementAndGet() == 1) {
                    instrumentMeta["first_hit_at"] = System.currentTimeMillis().toString()
                }
                instrumentMeta["last_hit_at"] = System.currentTimeMillis().toString()
            }

            val devInstrument = liveInstrument ?: liveInstrumentProcessor.getCachedDeveloperInstrument(hit.breakpointId)
            InstrumentProcessor.publishEvent(
                toLiveInstrumentSubscriberAddress(devInstrument.developerAuth.selfId),
                JsonObject.mapFrom(LiveInstrumentEvent(LiveInstrumentEventType.BREAKPOINT_HIT, Json.encode(hit)))
            )
            if (log.isTraceEnabled) log.trace("Published live breakpoint hit")
        }
    }

    private val listener: AnalysisListener = Listener()
    override fun create(p0: ModuleManager, p1: AnalyzerModuleConfig): AnalysisListener = listener
}
