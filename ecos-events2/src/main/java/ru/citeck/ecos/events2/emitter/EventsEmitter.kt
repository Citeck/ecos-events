package ru.citeck.ecos.events2.emitter

import java.util.*

class EventsEmitter<T : Any>(
    val config: EmitterConfig<T>,
    private val emitImpl: (Any) -> UUID
) {
    fun emit(eventData: T): UUID {
        return emitImpl.invoke(eventData)
    }
}
