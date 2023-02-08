package ru.citeck.ecos.events2.remote

data class RemoteEventListenerKey(
    val eventType: String,
    val exclusive: Boolean,
    val transactional: Boolean
)
