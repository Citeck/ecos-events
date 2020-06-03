package ru.citeck.ecos.events.emitter

class EmitterConfig<T : Any>(
    val family: String = "",
    val eventType: String,
    val eventClass: Class<T>,
    val isLocal: Boolean
) {
    class Builder<T : Any> {

        var family: String = ""
        lateinit var eventType: String
        lateinit var eventClass: Class<T>
        var isLocal: Boolean? = null

        fun build() : EmitterConfig<T> {
            return EmitterConfig(
                family,
                eventType,
                eventClass,
                isLocal!!
            )
        }
    }
}
