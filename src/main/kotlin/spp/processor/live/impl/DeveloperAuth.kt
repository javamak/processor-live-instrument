package spp.processor.live.impl

import io.vertx.core.Handler
import io.vertx.core.eventbus.impl.MessageImpl
import io.vertx.ext.auth.impl.jose.JWT
import org.joor.Reflect

data class DeveloperAuth(
    val selfId: String,
    val accessToken: String?,
) {

    override fun toString(): String = selfId

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DeveloperAuth) return false
        if (selfId != other.selfId) return false
        return true
    }

    override fun hashCode(): Int = selfId.hashCode()

    companion object {
        fun from(selfId: String, accessToken: String?): DeveloperAuth {
            return DeveloperAuth(selfId, accessToken)
        }

        fun from(handler: Handler<*>): DeveloperAuth {
            val arg1 = Reflect.on(handler).get<Any?>("arg\$1")
            val arg2 = Reflect.on(handler).get<Any?>("arg\$2")
            val messageImpl = (if (arg1 is MessageImpl<*, *>) arg1 else arg2) as MessageImpl<*, *>
            return messageImpl.headers().let {
                if (it.contains("auth-token")) {
                    DeveloperAuth(
                        JWT.parse(it.get("auth-token")).getJsonObject("payload").getString("developer_id"),
                        it.get("auth-token")
                    )
                } else DeveloperAuth("system", null)
            }
        }
    }
}
