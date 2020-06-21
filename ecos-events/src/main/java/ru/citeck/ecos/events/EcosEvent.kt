package ru.citeck.ecos.events

import ru.citeck.ecos.records2.RecordRef
import java.time.Instant
import java.util.*

data class EcosEvent(
    val id: UUID,
    val time: Instant,
    val type: String,
    val recordRef: RecordRef,
    val user: String,
    val source: String,
    val sourceApp: String
)
