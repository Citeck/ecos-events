package ru.citeck.ecos.events.remote

data class RemoteListener(
    val eventType: String,
    val appName: String,
    val appInstance: String,
    val attributes: Set<String>
)