package ru.citeck.ecos.events2

import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import java.time.Instant
import java.util.*

data class EcosEvent(
    val id: UUID,
    val time: Instant,
    val type: String,
    val user: String,
    val source: EventSource,
    val attributes: RecordAtts
)
