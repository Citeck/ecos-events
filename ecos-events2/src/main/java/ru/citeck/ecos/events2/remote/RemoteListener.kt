package ru.citeck.ecos.events2.remote

data class RemoteListener(
    val eventType: String,
    val appName: String,
    val appInstance: String,
    val attributes: Set<String>
)