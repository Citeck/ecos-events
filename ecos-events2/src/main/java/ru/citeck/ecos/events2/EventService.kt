package ru.citeck.ecos.events2

import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.emitter.EventEmitter
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.listener.ListenerHandle

interface EventService {

    fun <T : Any> getEmitter(config: EmitterConfig<T>): EventEmitter<T>

    fun emitRemoteEvent(event: EcosEvent)

    fun addListener(listener: ListenerConfig<*>): ListenerHandle

    fun removeListener(listener: ListenerConfig<*>)

    fun removeListener(id: String)

}