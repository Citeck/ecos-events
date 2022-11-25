package ru.citeck.ecos.events2

import java.time.Instant
import java.util.*

data class EcosEventInfo(
    val id: UUID,
    val time: Instant,
    val type: String,
    val user: String,
    val source: EventsSource
)
