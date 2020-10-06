package ru.citeck.ecos.events

data class EventSource(
    val from: String,
    val tags: Set<String>,
    val appName: String,
    val appInstanceId: String
)
