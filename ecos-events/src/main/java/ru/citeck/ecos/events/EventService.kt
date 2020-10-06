package ru.citeck.ecos.events

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.events.emitter.EmitterConfig
import ru.citeck.ecos.events.emitter.EventEmitter
import ru.citeck.ecos.events.listener.ListenerConfig
import ru.citeck.ecos.events.listener.ListenerHandle
import ru.citeck.ecos.events.listener.ctx.EventTypeListeners
import ru.citeck.ecos.events.listener.ctx.ListenerInfo
import ru.citeck.ecos.events.listener.ctx.ListenersContext
import ru.citeck.ecos.records2.RecordMeta
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.meta.RecordsMetaService
import ru.citeck.ecos.records2.predicate.RecordElement
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import java.lang.IllegalStateException
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer

class EventService(serviceFactory: EventServiceFactory) {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private val remoteEvents = serviceFactory.remoteEvents

    private val emitters: MutableMap<EmitterConfig<*>, EventEmitter<*>> = ConcurrentHashMap()

    private val predicateService = serviceFactory.recordsServiceFactory.predicateService
    private var recordsMetaService: RecordsMetaService = serviceFactory.recordsServiceFactory.recordsMetaService

    private val listenersContext: ListenersContext = serviceFactory.listenersContext

    fun <T : Any> getEmitter(config: EmitterConfig<T>) : EventEmitter<T> {
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

    private fun emitExactEvent(event: EcosEvent,
                               listeners: EventTypeListeners,
                               isLocalEvent: Boolean) {

        listeners.listeners.forEach { listener ->
            if (isLocalEvent || !listener.config.local) {
                triggerListener(listener, event)
            }
        }
    }

    private fun emitRecordEvent(event: Any, config: EmitterConfig<*>) : UUID {

        val eventId = UUID.randomUUID()
        val time = Instant.now()
        val typeListeners = getListenersForType(config.eventType) ?: return eventId

        val fullDataAtts = recordsMetaService.getMeta(event, typeListeners.attributes).attributes

        val ecosEvent = EcosEvent(
                eventId,
                time,
                config.eventType,
                "current",
                config.source,
                "sourceApp",
                fullDataAtts
        )

        emitExactEvent(ecosEvent, typeListeners, true)
        return eventId
    }

    private fun getListenersForType(eventType: String) : EventTypeListeners? {

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
            event.attributes.forEach(BiConsumer { k, v ->
                if (k.startsWith(EventConstants.FILTER_ATT_PREFIX)) {
                    filterAtts.set(k.replaceFirst(EventConstants.FILTER_ATT_PREFIX, ""), v)
                }
            })
            val element = RecordElement(RecordMeta(RecordRef.EMPTY, filterAtts))
            if (!predicateService.isMatch(element, listener.config.filter)) {
                return
            }
        }

        val listenerAtts = ObjectData.create()

        listener.attributes.forEach { (k, v) ->
            listenerAtts.set(k, event.attributes.get(v))
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

    fun addListener(listener: ListenerConfig<*>) : ListenerHandle {
        return listenersContext.addListener(listener)
    }
}