package ru.citeck.ecos.events.listener.ctx

import ru.citeck.ecos.events.listener.ListenerConfig

class ListenerInfo(
    val attributes: Map<String, String>,
    val config: ListenerConfig<Any>
)