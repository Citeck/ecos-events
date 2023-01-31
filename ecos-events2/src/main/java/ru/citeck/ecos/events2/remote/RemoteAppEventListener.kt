package ru.citeck.ecos.events2.remote

import ru.citeck.ecos.records2.predicate.model.Predicate

data class RemoteAppEventListener(
    val targetAppKey: String,
    val attributes: Set<String>,
    val filter: Predicate,
    val transactional: Boolean = false
)
