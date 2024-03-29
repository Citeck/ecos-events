package ru.citeck.ecos.events2.listener

import ru.citeck.ecos.commons.utils.MandatoryParam
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.webapp.api.func.UncheckedConsumer
import java.util.*
import kotlin.collections.HashMap

data class ListenerConfig<T : Any>(
    /**
     * Listener identifier.
     * Used to delete registered listeners by ID.
     */
    val id: String,
    /**
     * Event type to listen
     */
    val eventType: String,
    /**
     * Data class for action.
     */
    val dataClass: Class<T>,
    /**
     * Attributes to load. Can be omitted.
     * This attributes can replace attributes from dataClass if map key named as field with setter in dataClass
     */
    val attributes: Map<String, String>,
    /**
     * Should we listen global events between applications or only local events?
     */
    val local: Boolean,
    /**
     * Action which will be invoked when event occurred.
     */
    val action: UncheckedConsumer<T>,
    /**
     * Filter for events.
     */
    val filter: Predicate,
    /**
     * Should every instance of the same application receive separate event
     * (exclusive = false) or each event processed by single application (exclusive = true).
     * Not exclusive listeners can't be consistent
     */
    var exclusive: Boolean,
    /**
     * Is this listener should be triggered within
     * transaction synchronously or not
     */
    var transactional: Boolean
) {

    companion object {

        private val EMPTY = create<Any> {
            withDataClass(Any::class.java)
            withAction {}
        }

        @JvmStatic
        fun <T : Any> create(): Builder<T> {
            return Builder()
        }

        @JvmStatic
        fun <T : Any> create(block: Builder<T>.() -> Unit): ListenerConfig<T> {
            val builder = Builder<T>()
            block.invoke(builder)
            return builder.build()
        }
    }

    fun copy(): Builder<T> {
        return Builder(this)
    }

    class Builder<T : Any>() {

        var id: String = ""
        var eventType: String = ""
        var dataClass: Class<T>? = null
        var attributes: MutableMap<String, String> = HashMap()
        var action: UncheckedConsumer<T>? = null
        var filter: Predicate = VoidPredicate.INSTANCE
        var local: Boolean = false
        var exclusive: Boolean = true
        var transactional: Boolean = false

        constructor(base: ListenerConfig<T>) : this() {
            this.id = ""
            this.eventType = base.eventType
            this.dataClass = base.dataClass
            this.attributes = HashMap(base.attributes)
            this.action = base.action
            this.filter = base.filter
            this.local = base.local
            this.exclusive = base.exclusive
            this.transactional = base.transactional
        }

        fun withId(id: String?): Builder<T> {
            this.id = id ?: EMPTY.id
            return this
        }

        fun withEventType(eventType: String?): Builder<T> {
            this.eventType = eventType ?: EMPTY.eventType
            return this
        }

        fun withDataClass(dataClass: Class<T>?): Builder<T> {
            this.dataClass = dataClass
            return this
        }

        fun withAttributes(attributes: Map<String, String>?): Builder<T> {
            this.attributes = attributes?.let { HashMap(it) } ?: HashMap()
            return this
        }

        fun withActionJ(action: UncheckedConsumer<T>?): Builder<T> {
            this.action = action
            return this
        }

        fun withAction(action: ((T) -> Unit)?): Builder<T> {
            if (action == null) {
                this.action = null
            } else {
                this.action = object : UncheckedConsumer<T> {
                    override fun accept(arg: T) {
                        action.invoke(arg)
                    }
                }
            }
            return this
        }

        fun withFilter(filter: Predicate?): Builder<T> {
            this.filter = filter ?: EMPTY.filter
            return this
        }

        fun withLocal(local: Boolean?): Builder<T> {
            this.local = local ?: EMPTY.local
            return this
        }

        fun withExclusive(exclusive: Boolean?): Builder<T> {
            this.exclusive = exclusive ?: EMPTY.exclusive
            return this
        }

        fun withTransactional(transactional: Boolean?): Builder<T> {
            this.transactional = transactional ?: true
            return this
        }

        fun addAttribute(key: String, value: String) {
            attributes[key] = value
        }

        fun addAttributes(data: Map<String, String>) {
            attributes.putAll(data)
        }

        @Deprecated(
            "use withAction builder method",
            replaceWith = ReplaceWith("withAction(action)")
        )
        fun setAction(action: (T) -> Unit) {
            withAction(action)
        }

        fun build(): ListenerConfig<T> {

            MandatoryParam.check("dataClass", dataClass)
            MandatoryParam.check("action", action)

            if (transactional && !exclusive) {
                error("Listener should be exclusive when transactional flag enabled")
            }

            return ListenerConfig(
                id.ifBlank { UUID.randomUUID().toString() },
                eventType,
                dataClass!!,
                attributes,
                local,
                action!!,
                filter,
                exclusive,
                transactional
            )
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as ListenerConfig<*>
        if (id != other.id) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }
}
