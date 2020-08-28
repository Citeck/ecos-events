package ru.citeck.ecos.events

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.events.emitter.EmitterConfig
import ru.citeck.ecos.events.emitter.EventEmitter
import ru.citeck.ecos.events.listener.ListenerConfig
import ru.citeck.ecos.events.listener.ListenerHandler
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
import kotlin.collections.HashMap

class EventService(serviceFactory: EventServiceFactory) {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private val remoteEvents = serviceFactory.remoteEvents

    private val emitters: MutableMap<EmitterConfig<*>, EventEmitter<*>> = ConcurrentHashMap()

    private val recordsService = serviceFactory.recordsServiceFactory.recordsService
    private val predicateService = serviceFactory.recordsServiceFactory.predicateService
    private var recordsMetaService: RecordsMetaService = serviceFactory.recordsServiceFactory.recordsMetaService

    private val listenersContext: ListenersContext = serviceFactory.listenersContext

    fun <T : Any> getEmitter(builder: EmitterConfig.Builder<T>.() -> Unit) : EventEmitter<T> {
        return getEmitter(EmitterConfig.create(builder))
    }

    fun <T : Any> getEmitter(config: EmitterConfig<T>) : EventEmitter<T> {
        val emitter = emitters.computeIfAbsent(config) {
            remoteEvents?.addProducedEventType(config.eventType)
            EventEmitter(config) { record, event -> emitRecordEvent(record, event, config) }
        }
        @Suppress("UNCHECKED_CAST")
        return emitter as EventEmitter<T>
    }

    fun emitRemoteEvent(event: EcosEvent, attributes: ObjectData) {
        val typeListeners = getListenersForType(event.type) ?: return
        emitExactEvent(event, attributes, typeListeners, false)
    }

    private fun emitExactEvent(event: EcosEvent,
                               attributes: ObjectData,
                               listeners: EventTypeListeners,
                               isLocalEvent: Boolean) {

        listeners.listeners.forEach { listener ->
            if (isLocalEvent || !listener.config.local) {
                triggerListener(listener, attributes, event)
            }
        }
    }

    private fun emitRecordEvent(recordRef: RecordRef, event: Any, config: EmitterConfig<*>) : UUID {

        val eventId = UUID.randomUUID()
        val time = Instant.now()
        val typeListeners = getListenersForType(config.eventType) ?: return eventId

        val fullDataAtts = ObjectData.create()

        if (typeListeners.recordAtts.isNotEmpty()) {
            val attributes = recordsService.getAttributes(recordRef, typeListeners.recordAtts)
            attributes.forEach { k, v -> fullDataAtts.set(k, v) }
        }

        if (typeListeners.modelAtts.isNotEmpty()) {

            val model = HashMap<String, Any>()
            model["event"] = event

            val modelAttributes = recordsMetaService.getMeta(model, typeListeners.modelAtts)
            modelAttributes.forEach { k, v -> fullDataAtts.set(k, v) }
        }

        val ecosEvent = EcosEvent(
                eventId,
                time,
                config.eventType,
                recordRef,
                "current",
                config.source,
                "sourceApp"
        )

        emitExactEvent(ecosEvent, fullDataAtts, typeListeners, true)
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

    private fun triggerListener(listener: ListenerInfo,
                                fullData: ObjectData,
                                event: EcosEvent) {

        if (listener.config.filter !is VoidPredicate) {
            val filterAtts = ObjectData.create()
            fullData.forEach(BiConsumer { k, v ->
                if (k.startsWith(EventConstants.FILTER_ATT_PREFIX)) {
                    filterAtts.set(k.replaceFirst(EventConstants.FILTER_ATT_PREFIX, ""), v)
                }
            })
            val element = RecordElement(RecordMeta(event.recordRef, filterAtts))
            if (!predicateService.isMatch(element, listener.config.filter)) {
                return
            }
        }

        val listenerAtts = ObjectData.create()

        listener.attributes.forEach { (k, v) ->
            listenerAtts.set(k, fullData.get(v))
        }

        val clazz = listener.config.dataClass
        val action = listener.config.action

        if (clazz == RecordRef::class.java) {
            action.accept(event.recordRef, event)
        } else if (clazz == ObjectData::class.java) {
            action.accept(listenerAtts, event)
        } else if (clazz == Unit::class.java) {
            action.accept(Unit, event)
        } else {
            val converted = Json.mapper.convert(listenerAtts, clazz)
            if (converted != null) {
                action.accept(converted, event)
            } else {
                throw IllegalStateException("Event data can't be converted to $clazz. Data: $listenerAtts");
            }
        }
    }

    fun addListener(listener: ListenerConfig<*>) : ListenerHandler {
        return listenersContext.addListener(listener)
    }
}