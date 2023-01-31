package ru.citeck.ecos.events2.rabbitmq

import ru.citeck.ecos.records2.predicate.model.Predicate

data class ZkAppEventListener(
    val attributes: Set<String>,
    val filter: Predicate,
    val transactional: Boolean = false
)
