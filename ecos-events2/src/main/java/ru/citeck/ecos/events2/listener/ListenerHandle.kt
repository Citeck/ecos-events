package ru.citeck.ecos.events2.listener

import ru.citeck.ecos.events2.listener.ctx.ListenersContext

class ListenerHandle(
    val config: ListenerConfig<*>,
    val context: ListenersContext
) {
    fun remove() {
        context.removeListener(config)
    }
}
