package ru.citeck.ecos.events2

import ru.citeck.ecos.commons.data.ObjectData
import java.time.Instant
import java.util.*

data class EcosEvent(
    val id: UUID,
    val time: Instant,
    val type: String,
    val user: String,
    val source: EventsSource,
    val attributes: ObjectData
) {

    fun withAttributes(attributes: ObjectData): EcosEvent {
        return EcosEvent(
            id,
            time,
            type,
            user,
            source,
            attributes
        )
    }
}
