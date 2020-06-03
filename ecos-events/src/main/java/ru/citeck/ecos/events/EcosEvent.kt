package ru.citeck.ecos.events

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.records2.RecordRef
import java.time.Instant

data class EcosEvent(
    val id: String,
    val time: Instant,
    val type: String,
    val record: RecordRef,
    val user: String,
    val attributes: ObjectData
)
