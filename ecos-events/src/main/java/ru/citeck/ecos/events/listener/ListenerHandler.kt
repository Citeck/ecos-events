package ru.citeck.ecos.events.listener

import ru.citeck.ecos.events.listener.ctx.ListenersContext

class ListenerHandler(
    val config: ListenerConfig<*>,
    val context: ListenersContext
) {
    fun remove() {
        context.removeListener(config)
    }
}