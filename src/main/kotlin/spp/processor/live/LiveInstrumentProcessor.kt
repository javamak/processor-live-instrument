package spp.processor.live

import spp.protocol.instrument.meter.LiveMeter
import io.vertx.codegen.annotations.ProxyGen
import io.vertx.codegen.annotations.VertxGen
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject
import kotlinx.datetime.Instant

@VertxGen
@ProxyGen
interface LiveInstrumentProcessor {
    fun setupLiveMeter(
        liveMeter: LiveMeter,
        handler: Handler<AsyncResult<JsonObject>>
    )

    fun getLiveMeterMetrics(
        liveMeter: LiveMeter,
        start: Instant,
        stop: Instant,
        step: String,
        handler: Handler<AsyncResult<JsonObject>>
    )
}
