package ru.citeck.ecos.events2

import ru.citeck.ecos.commons.data.ObjectData
import java.time.Instant
import java.util.*

data class EcosEvent(
    val id: UUID,
    val time: Instant,
    val type: String,
    val user: String,
    val source: EventSource,
    val attributes: ObjectData
)
