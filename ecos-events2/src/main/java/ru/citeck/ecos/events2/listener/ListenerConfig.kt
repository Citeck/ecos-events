package ru.citeck.ecos.events2.listener

import ru.citeck.ecos.commons.utils.func.UncheckedConsumer
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import java.util.*
import kotlin.collections.HashMap

data class ListenerConfig<T : Any>(
    val id: String,
    val eventType: String,
    val dataClass: Class<T>,
    val attributes: Map<String, String>,
    val local: Boolean,
    val action: UncheckedConsumer<T>,
    val filter: Predicate,
    val consistent: Boolean
) {

    companion object {

        fun <T : Any> create(block: Builder<T>.() -> Unit): ListenerConfig<T> {
            val builder = Builder<T>()
            block.invoke(builder)
            return builder.build()
        }
    }

    class Builder<T : Any> {

        var id: String? = null
        var eventType: String? = null
        var dataClass: Class<T>? = null
        var attributes: MutableMap<String, String> = HashMap()
        var local: Boolean = false
        var action: UncheckedConsumer<T>? = null
        var filter: Predicate = VoidPredicate.INSTANCE
        var consistent: Boolean = true

        fun addAttribute(key: String, value: String) {
            attributes[key] = value
        }

        fun addAttributes(data: Map<String, String>) {
            attributes.putAll(data)
        }

        fun setAction(action: (T) -> Unit) {
            this.action = object : UncheckedConsumer<T> {
                override fun accept(arg: T) {
                   action.invoke(arg)
                }
            }
        }

        fun build(): ListenerConfig<T> {
            if (id.isNullOrBlank()) {
                id = UUID.randomUUID().toString()
            }

            return ListenerConfig(
                id!!,
                eventType!!,
                dataClass!!,
                attributes,
                local,
                action!!,
                filter,
                consistent
            )
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ListenerConfig<*>

        if (id != other.id) return false

        return true
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }


}