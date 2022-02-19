package spp.processor.live.impl.instrument

import org.joor.Reflect
import spp.protocol.instrument.variable.LiveVariable
import java.time.*
import java.util.*
import kotlin.reflect.jvm.jvmName

object ReflectiveValueFormatter {

    private val supportedClasses = mapOf<String, () -> Reflect>(
        Date::class.jvmName to { Reflect.on(Date()) },
        Duration::class.jvmName to { Reflect.on(Duration.ofSeconds(1)) },
        Instant::class.jvmName to { Reflect.on(Instant.now()) },
        LocalDate::class.jvmName to { Reflect.on(LocalDate.now()) },
        LocalTime::class.jvmName to { Reflect.on(LocalTime.now()) },
        LocalDateTime::class.jvmName to { Reflect.on(LocalDateTime.now()) },
        OffsetDateTime::class.jvmName to { Reflect.on(OffsetDateTime.now()) },
        OffsetTime::class.jvmName to { Reflect.on(OffsetTime.now()) },
        ZonedDateTime::class.jvmName to { Reflect.on(ZonedDateTime.now()) },
        ZoneOffset::class.jvmName to { Reflect.on(ZoneOffset.ofTotalSeconds(0)) },
        "java.time.ZoneRegion" to { Reflect.onClass("java.time.ZoneRegion").call("ofId", "GMT", false) }
    )

    fun format(liveClazz: String?, variable: LiveVariable) : String? {
        if (liveClazz !in supportedClasses)
            return ""
        val obj : String? = when (variable.value) {
            is String -> variable.value as String
            is List<*> -> setValues(liveClazz, variable.value as List<LiveVariable>)?.toString()
            else -> ""
        }
        return obj

        return ""
    }

    private fun setValues(liveClazz: String?, values: List<LiveVariable>): Reflect? {
        val obj = supportedClasses[liveClazz]?.invoke()
        obj?.let {
            values.forEach {
                if (it.liveClazz != null) {
                    val childObj = setValues(it.liveClazz as String, it.value as List<LiveVariable>)

                    obj.set(it.name as String, childObj)
                } else {
                    obj.set(it.name as String, asSmallestObject(it.value))
                }
            }
        }
        return obj
    }

    //todo: this should be done by processor
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
}