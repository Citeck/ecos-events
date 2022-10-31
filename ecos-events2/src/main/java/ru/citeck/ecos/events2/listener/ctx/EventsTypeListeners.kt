package ru.citeck.ecos.events2.listener.ctx

data class EventsTypeListeners(
    val attributes: Set<String>,
    val listeners: List<ListenerInfo>
)
