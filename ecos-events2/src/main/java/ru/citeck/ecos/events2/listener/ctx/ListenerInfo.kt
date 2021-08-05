package ru.citeck.ecos.events2.listener.ctx

import ru.citeck.ecos.events2.listener.ListenerConfig

data class ListenerInfo(
    val attributes: Map<String, String>,
    val config: ListenerConfig<Any>
)