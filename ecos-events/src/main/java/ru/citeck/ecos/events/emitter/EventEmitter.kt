package ru.citeck.ecos.events.emitter

import java.util.*

class EventEmitter<T : Any>(
    val config: EmitterConfig<T>,
    private val emitImpl: (Any) -> UUID
) {
    fun emit(eventData: T) : UUID {
        return emitImpl.invoke(eventData)
    }
}