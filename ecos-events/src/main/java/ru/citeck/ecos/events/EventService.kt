package ru.citeck.ecos.events

import ru.citeck.ecos.events.emitter.EmitterConfig
import ru.citeck.ecos.events.emitter.EventEmitter
import ru.citeck.ecos.events.listener.EventListenerConfig
import ru.citeck.ecos.records2.RecordRef
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class EventService {

    private val emitters: MutableMap<EmitterConfig<*>, EventEmitter<*>> = ConcurrentHashMap()
    //private val listeners: List<>

    fun <T : Any> getEmitter(block: EmitterConfig.Builder<T>.() -> Unit) : EventEmitter<T> {
        val builder = EmitterConfig.Builder<T>()
        block.invoke(builder)
        return getEmitter(builder.build())
    }

    fun <T : Any> getEmitter(config: EmitterConfig<T>) : EventEmitter<T> {
        val emitter = emitters.computeIfAbsent(config) {
            EventEmitter(config) { source, record, event -> fireEventImpl(source, record, event, config) }
        }
        @Suppress("UNCHECKED_CAST")
        return emitter as EventEmitter<T>
    }

    private fun fireEventImpl(source: String, record: RecordRef, event: Any, config: EmitterConfig<*>) : UUID {

    }

    fun addListener(config: EventListenerConfig) {

    }
}