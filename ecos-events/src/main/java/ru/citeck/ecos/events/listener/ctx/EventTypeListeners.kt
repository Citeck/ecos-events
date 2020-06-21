package ru.citeck.ecos.events.listener.ctx

class EventTypeListeners(
    val recordAtts: Set<String>,
    val modelAtts: Map<String, String>,
    val listeners: List<ListenerInfo>
)