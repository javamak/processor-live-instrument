package spp.processor

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.eventbus.ReplyFailure
import io.vertx.core.json.Json
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
import spp.processor.InstrumentProcessor.requestEvent
import spp.processor.common.FeedbackProcessor
import spp.processor.common.FeedbackProcessor.Companion.INSTANCE_ID
import spp.processor.live.impl.LiveInstrumentProcessorImpl
import spp.protocol.SourceMarkerServices
import spp.protocol.auth.RolePermission
import spp.protocol.auth.RolePermission.*
import spp.protocol.developer.SelfInfo
import spp.protocol.service.error.InstrumentAccessDenied
import spp.protocol.service.error.PermissionAccessDenied
import spp.protocol.service.live.LiveInstrumentService
import spp.protocol.util.AccessChecker
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
            .addInterceptor { msg -> permissionAndAccessCheckInterceptor(msg) }
            .setAddress(SourceMarkerServices.Utilize.LIVE_INSTRUMENT)
            .register(LiveInstrumentService::class.java, liveInstrumentProcessor)
        liveInstrumentRecord = EventBusService.createRecord(
            SourceMarkerServices.Utilize.LIVE_INSTRUMENT,
            SourceMarkerServices.Utilize.LIVE_INSTRUMENT,
            LiveInstrumentService::class.java,
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

    private fun permissionAndAccessCheckInterceptor(msg: Message<JsonObject>): Future<Message<JsonObject>> {
        val promise = Promise.promise<Message<JsonObject>>()
        requestEvent(
            vertx, SourceMarkerServices.Utilize.LIVE_SERVICE, JsonObject(),
            JsonObject().put("auth-token", msg.headers().get("auth-token")).put("action", "getSelf")
        ) {
            if (it.succeeded()) {
                val selfInfo = Json.decodeValue(it.result().toString(), SelfInfo::class.java)
                validateRolePermission(selfInfo, msg) {
                    if (it.succeeded()) {
                        if (msg.headers().get("action").startsWith("addLiveInstrument")) {
                            validateInstrumentAccess(selfInfo, msg, promise)
                        } else {
                            promise.complete(msg)
                        }
                    } else {
                        promise.fail(it.cause())
                    }
                }
            } else {
                promise.fail(it.cause())
            }
        }
        return promise.future()
    }

    private fun validateRolePermission(
        selfInfo: SelfInfo, msg: Message<JsonObject>, handler: Handler<AsyncResult<Message<JsonObject>>>
    ) {
        if (msg.headers().get("action") == "addLiveInstruments") {
            val batchPromise = Promise.promise<Message<JsonObject>>()
            msg.body().getJsonObject("batch").getJsonArray("instruments").list.forEach {
                val instrumentType = (it as JsonObject).getString("type")
                val necessaryPermission = RolePermission.valueOf("ADD_LIVE_$instrumentType")
                if (!selfInfo.permissions.contains(necessaryPermission)) {
                    batchPromise.fail(PermissionAccessDenied(necessaryPermission).toEventBusException())
                }
            }
            batchPromise.tryComplete(msg)
            handler.handle(batchPromise.future())
        } else if (msg.headers().get("action") == "addLiveInstrument") {
            val instrumentType = msg.body().getJsonObject("instrument").getString("type")
            val necessaryPermission = RolePermission.valueOf("ADD_LIVE_$instrumentType")
            if (selfInfo.permissions.contains(necessaryPermission)) {
                handler.handle(Future.succeededFuture(msg))
            } else {
                handler.handle(Future.failedFuture(PermissionAccessDenied(necessaryPermission).toEventBusException()))
            }
        } else if (msg.headers().get("action").startsWith("removeLiveInstrument")) {
            if (selfInfo.permissions.contains(REMOVE_LIVE_INSTRUMENT)) {
                handler.handle(Future.succeededFuture(msg))
            } else {
                handler.handle(Future.failedFuture(PermissionAccessDenied(REMOVE_LIVE_INSTRUMENT).toEventBusException()))
            }
        } else if (msg.headers().get("action").startsWith("getLiveInstrument")) {
            if (selfInfo.permissions.contains(GET_LIVE_INSTRUMENTS)) {
                handler.handle(Future.succeededFuture(msg))
            } else {
                handler.handle(Future.failedFuture(PermissionAccessDenied(GET_LIVE_INSTRUMENTS).toEventBusException()))
            }
        } else if (msg.headers().get("action") == "clearLiveInstruments") {
            if (selfInfo.permissions.contains(CLEAR_ALL_LIVE_INSTRUMENTS)) {
                handler.handle(Future.succeededFuture(msg))
            } else {
                handler.handle(Future.failedFuture(PermissionAccessDenied(CLEAR_ALL_LIVE_INSTRUMENTS).toEventBusException()))
            }
        } else if (RolePermission.fromString(msg.headers().get("action")) != null) {
            val necessaryPermission = RolePermission.fromString(msg.headers().get("action"))!!
            if (selfInfo.permissions.contains(necessaryPermission)) {
                handler.handle(Future.succeededFuture(msg))
            } else {
                handler.handle(Future.failedFuture(PermissionAccessDenied(necessaryPermission).toEventBusException()))
            }
        } else {
            TODO()
        }
    }

    private fun validateInstrumentAccess(
        selfInfo: SelfInfo, msg: Message<JsonObject>, promise: Promise<Message<JsonObject>>
    ) {
        if (msg.headers().get("action") == "addLiveInstruments") {
            val instruments = msg.body().getJsonObject("batch").getJsonArray("instruments")
            for (i in 0 until instruments.size()) {
                val sourceLocation = instruments.getJsonObject(i)
                    .getJsonObject("location").getString("source")
                if (!AccessChecker.hasInstrumentAccess(selfInfo.access, sourceLocation)) {
                    log.warn(
                        "Rejected developer {} unauthorized instrument access to: {}",
                        selfInfo.developer.id, sourceLocation
                    )
                    val replyEx = ReplyException(
                        ReplyFailure.RECIPIENT_FAILURE, 403,
                        Json.encode(InstrumentAccessDenied(sourceLocation).toEventBusException())
                    )
                    promise.fail(replyEx)
                    return
                }
            }
            promise.complete(msg)
        } else {
            val sourceLocation = msg.body().getJsonObject("instrument")
                .getJsonObject("location").getString("source")
            if (!AccessChecker.hasInstrumentAccess(selfInfo.access, sourceLocation)) {
                log.warn(
                    "Rejected developer {} unauthorized instrument access to: {}",
                    selfInfo.developer.id, sourceLocation
                )
                promise.fail(InstrumentAccessDenied(sourceLocation).toEventBusException())
            } else {
                promise.complete(msg)
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
