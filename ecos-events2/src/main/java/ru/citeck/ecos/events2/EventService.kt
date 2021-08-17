package ru.citeck.ecos.events2

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.emitter.EventEmitter
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.listener.ListenerHandle
import ru.citeck.ecos.events2.listener.ctx.EventTypeListeners
import ru.citeck.ecos.events2.listener.ctx.ListenerInfo
import ru.citeck.ecos.events2.listener.ctx.ListenersContext
import ru.citeck.ecos.records2.RecordMeta
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.RecordElement
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class EventService(serviceFactory: EventServiceFactory) {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private val remoteEvents = serviceFactory.remoteEvents

    private val emitters: MutableMap<EmitterConfig<*>, EventEmitter<*>> = ConcurrentHashMap()

    private val predicateService = serviceFactory.recordsServiceFactory!!.predicateService
    private val recordsService = serviceFactory.recordsServiceFactory!!.recordsServiceV1

    private val listenersContext: ListenersContext = serviceFactory.listenersContext

    fun <T : Any> getEmitter(config: EmitterConfig<T>): EventEmitter<T> {
        val emitter = emitters.computeIfAbsent(config) {
            remoteEvents?.addProducedEventType(config.eventType)
            EventEmitter(config) { event -> emitRecordEvent(event, config) }
        }
        @Suppress("UNCHECKED_CAST")
        return emitter as EventEmitter<T>
    }

    fun emitRemoteEvent(event: EcosEvent) {
        val typeListeners = getListenersForType(event.type) ?: return
        emitExactEvent(event, typeListeners, false)
    }

    private fun emitExactEvent(
        event: EcosEvent,
        listeners: EventTypeListeners,
        isLocalEvent: Boolean
    ) {

        listeners.listeners.forEach { listener ->
            if (isLocalEvent || !listener.config.local) {
                triggerListener(listener, event)
            }
        }
    }

    private fun emitRecordEvent(event: Any, config: EmitterConfig<*>): UUID {

        val eventId = UUID.randomUUID()
        val time = Instant.now()
        val typeListeners = getListenersForType(config.eventType) ?: return eventId

        val fullDataAtts = recordsService.getAtts(event, typeListeners.attributes)

        val ecosEvent = EcosEvent(
            eventId,
            time,
            config.eventType,
            "current",
            EventSource(config.source, emptySet(), "", ""),
            fullDataAtts
        )

        emitExactEvent(ecosEvent, typeListeners, true)
        return eventId
    }

    private fun getListenersForType(eventType: String): EventTypeListeners? {

        val typeListeners = this.listenersContext.getListeners(eventType)

        if (typeListeners == null) {
            log.warn { "Listeners doesn't found for type $eventType" }
            return null
        }
        return typeListeners
    }

    private fun triggerListener(listener: ListenerInfo, event: EcosEvent) {

        if (listener.config.filter !is VoidPredicate) {
            val filterAtts = ObjectData.create()

            event.attributes.forEach { key, dataValue ->
                if (key.startsWith(EventConstants.FILTER_ATT_PREFIX)) {
                    filterAtts.set(key.replaceFirst(EventConstants.FILTER_ATT_PREFIX, ""), dataValue)
                }
            }

            val element = RecordElement(RecordMeta(RecordRef.EMPTY, filterAtts))
            if (!predicateService.isMatch(element, listener.config.filter)) {
                return
            }
        }

        val listenerAtts = ObjectData.create()

        listener.attributes.forEach { (k, v) ->
            listenerAtts.set(k, event.attributes.getAtt(v))
        }

        val clazz = listener.config.dataClass
        val action = listener.config.action

        if (clazz == RecordRef::class.java) {
            action.accept(event)
        } else if (clazz == ObjectData::class.java) {
            action.accept(listenerAtts)
        } else if (clazz == Unit::class.java) {
            action.accept(Unit)
        } else if (clazz == EcosEvent::class.java) {
            action.accept(event)
        } else {
            val converted = Json.mapper.convert(listenerAtts, clazz)
            if (converted != null) {
                action.accept(converted)
            } else {
                throw IllegalStateException("Event data can't be converted to $clazz. Data: $listenerAtts");
            }
        }
    }

    fun addListener(listener: ListenerConfig<*>): ListenerHandle {
        return listenersContext.addListener(listener)
    }

    fun removeListener(listener: ListenerConfig<*>) {
        return listenersContext.removeListener(listener)
    }

    fun removeListener(id: String) {
        return listenersContext.removeListener(id)
    }

}