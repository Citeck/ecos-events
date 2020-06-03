package ru.citeck.ecos.events.emitter

import ru.citeck.ecos.records2.RecordRef
import java.util.*

class EventEmitter<T : Any>(
    val config: EmitterConfig<T>,
    private val emitImpl: (String, RecordRef, Any) -> UUID
) {
    fun emit(source: String, recordRef: RecordRef, eventData: T) : UUID {
        return emitImpl.invoke(source, recordRef, eventData)
    }
}