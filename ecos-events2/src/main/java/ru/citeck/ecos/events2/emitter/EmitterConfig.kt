package ru.citeck.ecos.events2.emitter

class EmitterConfig<T : Any>(
    val family: String,
    val source: String,
    val eventType: String,
    val eventClass: Class<T>
) {

    companion object {

        fun <T : Any> create(block: Builder<T>.() -> Unit) : EmitterConfig<T> {
            val builder = Builder<T>()
            block.invoke(builder)
            return builder.build()
        }
    }

    class Builder<T : Any> {

        var family: String = ""
        var source: String = ""
        var eventType: String? = null
        var eventClass: Class<T>? = null

        fun build() : EmitterConfig<T> {
            return EmitterConfig(
                family,
                source,
                eventType!!,
                eventClass!!
            )
        }
    }
}
