package ru.citeck.ecos.events.emitter

import ru.citeck.ecos.records2.RecordRef
import java.util.*

class EventEmitter<T : Any>(
    val config: EmitterConfig<T>,
    private val emitImpl: (RecordRef, Any) -> UUID
) {
    fun emit(recordRef: RecordRef, eventData: T) : UUID {
        return emitImpl.invoke(recordRef, eventData)
    }
}