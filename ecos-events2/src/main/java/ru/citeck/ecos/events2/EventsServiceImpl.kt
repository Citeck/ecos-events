package ru.citeck.ecos.events2

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.emitter.EventsEmitter
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.listener.ListenerHandle
import ru.citeck.ecos.events2.listener.ctx.EventsTypeListeners
import ru.citeck.ecos.events2.listener.ctx.ListenerInfo
import ru.citeck.ecos.events2.listener.ctx.ListenersContext
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.element.elematts.RecordAttsElement
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.request.RequestContext
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class EventsServiceImpl(serviceFactory: EventsServiceFactory) : EventService, EventsService {

    companion object {
        const val EVENT_ATTR = "event"

        private val log = KotlinLogging.logger {}
    }

    private val remoteEvents = serviceFactory.remoteEvents

    private val emitters: MutableMap<EmitterConfig<*>, EventsEmitter<*>> = ConcurrentHashMap()

    private val predicateService = serviceFactory.recordsServices.predicateService
    private val recordsService = serviceFactory.recordsServices.recordsServiceV1

    private val listenersContext: ListenersContext = serviceFactory.listenersContext

    private val appName: String
    private val appInstanceId: String

    init {
        val props = serviceFactory.recordsServices.properties
        appName = props.appName
        appInstanceId = props.appInstanceId
    }

    override fun <T : Any> getEmitter(config: EmitterConfig<T>): EventsEmitter<T> {
        val emitter = emitters.computeIfAbsent(config) {
            remoteEvents?.addProducedEventType(config.eventType)
            EventsEmitter(config) { event -> emitRecordEvent(event, config) }
        }
        @Suppress("UNCHECKED_CAST")
        return emitter as EventsEmitter<T>
    }

    override fun emitEventFromRemote(event: EcosEvent) {
        val typeListeners = getListenersForType(event.type) ?: return
        emitExactEvent(event, typeListeners, false)
    }

    private fun emitExactEvent(
        event: EcosEvent,
        listeners: EventsTypeListeners,
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
        val user = AuthContext.getCurrentUser()
        val typeListeners = getListenersForType(config.eventType) ?: return eventId
        val eventsSource = EventsSource(config.source, appName, appInstanceId)

        val eventInfo = EcosEventInfo(
            id = eventId,
            time = time,
            user = user,
            source = eventsSource
        )

        val fullDataAtts = RequestContext.doWithAtts(
            mapOf(
                EVENT_ATTR to eventInfo
            )
        ) { _ ->
            AuthContext.runAsSystem {
                recordsService.getAtts(event, typeListeners.attributes)
            }
        }

        val ecosEvent = EcosEvent(
            eventId,
            time,
            config.eventType,
            user,
            eventsSource,
            fullDataAtts
        )

        emitExactEvent(ecosEvent, typeListeners, true)
        return eventId
    }

    private fun getListenersForType(eventType: String): EventsTypeListeners? {

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
                if (key.startsWith(EventsConstants.FILTER_ATT_PREFIX)) {
                    filterAtts.set(key.replaceFirst(EventsConstants.FILTER_ATT_PREFIX, ""), dataValue)
                }
            }
            val element = RecordAttsElement.create(RecordAtts(RecordRef.EMPTY, filterAtts))
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

    override fun addListener(listener: ListenerConfig<*>): ListenerHandle {
        return listenersContext.addListener(listener)
    }

    override fun removeListener(listener: ListenerConfig<*>) {
        return listenersContext.removeListener(listener)
    }

    override fun removeListener(id: String) {
        return listenersContext.removeListener(id)
    }
}