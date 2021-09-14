package ru.citeck.ecos.events2.emitter

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize

@JsonDeserialize(builder = EmitterConfig.Builder::class)
class EmitterConfig<T : Any>(
    val source: String,
    val eventType: String,
    val eventClass: Class<T>
) {

    companion object {

        @JvmStatic
        fun <T: Any> create(): Builder<T> {
            return Builder()
        }

        @JvmStatic
        fun <T : Any> create(block: Builder<T>.() -> Unit) : EmitterConfig<T> {
            val builder = Builder<T>()
            block.invoke(builder)
            return builder.build()
        }
    }

    fun copy(): Builder<T> {
        return Builder(this)
    }

    class Builder<T : Any>() {

        var source: String = ""
        var eventType: String = ""
        var eventClass: Class<T>? = null

        constructor(base: EmitterConfig<T>) : this() {
            this.source = base.source
            this.eventType = base.eventType
            this.eventClass = base.eventClass
        }

        fun withSource(source: String?): Builder<T> {
            this.source = source ?: ""
            return this
        }

        fun withEventType(eventType: String?): Builder<T> {
            this.eventType = eventType ?: ""
            return this
        }

        fun withEventClass(eventClass: Class<T>?): Builder<T> {
            this.eventClass = eventClass
            return this
        }

        fun build() : EmitterConfig<T> {
            return EmitterConfig(
                source,
                eventType,
                eventClass!!
            )
        }
    }
}
