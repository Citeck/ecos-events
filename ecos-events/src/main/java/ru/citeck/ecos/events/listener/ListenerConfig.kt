package ru.citeck.ecos.events.listener

import ru.citeck.ecos.commons.utils.func.UncheckedBiConsumer
import ru.citeck.ecos.events.EcosEvent
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.VoidPredicate

data class ListenerConfig<T : Any>(
    val eventType: String,
    val dataClass: Class<T>,
    val attributes: Map<String, String>,
    val local: Boolean,
    val action: UncheckedBiConsumer<T, EcosEvent>,
    val filter: Predicate,
    val consistent: Boolean
) {

    companion object {

        fun <T : Any> create(block: Builder<T>.() -> Unit) : ListenerConfig<T> {
            val builder = Builder<T>()
            block.invoke(builder)
            return builder.build()
        }
    }

    class Builder<T : Any> {

        var eventType: String? = null
        var dataClass: Class<T>? = null
        var attributes: MutableMap<String, String> = HashMap()
        var local: Boolean = false
        var action: UncheckedBiConsumer<T, EcosEvent>? = null
        var filter: Predicate = VoidPredicate.INSTANCE
        var consistent: Boolean = true

        fun addAttribute(key: String, value: String) {
            attributes[key] = value
        }

        fun addAttributes(data: Map<String, String>) {
            attributes.putAll(data)
        }

        fun setAction(action: (T, EcosEvent) -> Unit) {
            this.action = object : UncheckedBiConsumer<T, EcosEvent> {
                override fun accept(arg0: T, arg1: EcosEvent) {
                    action.invoke(arg0, arg1)
                }
            }
        }

        fun build() : ListenerConfig<T> {
            return ListenerConfig(
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
}