package ru.citeck.ecos.events

import ru.citeck.ecos.commons.data.ObjectData
import java.time.Instant
import java.util.*

data class EcosEvent(
    val id: UUID,
    val time: Instant,
    val type: String,
    val user: String,
    val source: String,
    val sourceApp: String,
    val attributes: ObjectData
)
