package ru.citeck.ecos.events2

data class EventSource(
    val from: String,
    val tags: Set<String>,
    val appName: String,
    val appInstanceId: String
)
