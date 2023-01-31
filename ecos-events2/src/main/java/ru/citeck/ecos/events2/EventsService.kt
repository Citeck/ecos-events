package ru.citeck.ecos.events2

import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.emitter.EventsEmitter
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.listener.ListenerHandle
import ru.citeck.ecos.events2.listener.ctx.EventsTypeListeners

interface EventsService {

    fun <T : Any> getEmitter(config: EmitterConfig<T>): EventsEmitter<T>

    fun <T : Any> getEmitter(config: EmitterConfig.Builder<T>.() -> Unit): EventsEmitter<T>

    fun emitEventFromRemote(event: EcosEvent, exclusive: Boolean, calledInTxn: Boolean)

    fun addListener(listener: ListenerConfig<*>): ListenerHandle

    fun <T : Any> addListener(listener: ListenerConfig.Builder<T>.() -> Unit): ListenerHandle

    fun removeListener(listener: ListenerConfig<*>)

    fun removeListener(id: String)

    /**
     * Return listener only for reed-only operations
     */
    fun getListeners(): Map<String, EventsTypeListeners>
}
