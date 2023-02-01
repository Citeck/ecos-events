package ru.citeck.ecos.events2

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
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
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class EventsServiceImpl(serviceFactory: EventsServiceFactory) : EventsService {

    companion object {
        const val EVENT_ATTR = "event"

        private val log = KotlinLogging.logger {}
    }

    private val remoteEvents = serviceFactory.remoteEventsService
    private val recordsTemplateService = serviceFactory.recordsServices.recordsTemplateService

    private val emitters: MutableMap<EmitterConfig<*>, EventsEmitter<*>> = ConcurrentHashMap()

    private val predicateService = serviceFactory.recordsServices.predicateService
    private val recordsService = serviceFactory.recordsServices.recordsServiceV1
    private val dtoSchemaReader = serviceFactory.recordsServices.dtoSchemaReader

    private val listenersContext: ListenersContext = serviceFactory.listenersContext

    private val appName: String
    private val appInstanceId: String

    init {
        val props = serviceFactory.recordsServices.webappProps
        appName = props.appName
        appInstanceId = props.appInstanceId
    }

    override fun <T : Any> getEmitter(config: EmitterConfig.Builder<T>.() -> Unit): EventsEmitter<T> {
        val builder = EmitterConfig.create<T>()
        config.invoke(builder)
        return getEmitter(builder.build())
    }

    override fun <T : Any> getEmitter(config: EmitterConfig<T>): EventsEmitter<T> {
        val emitter = emitters.computeIfAbsent(config) {
            remoteEvents?.addProducedEventType(config.eventType)
            EventsEmitter(config) { event -> emitRecordEvent(event, config) }
        }
        @Suppress("UNCHECKED_CAST")
        return emitter as EventsEmitter<T>
    }

    override fun emitEventFromRemote(event: EcosEvent, exclusive: Boolean, calledInTxn: Boolean) {
        val typeListeners = getListenersForType(event.type) ?: return
        emitExactEvent(event, typeListeners, false, exclusive, calledInTxn)
    }

    private fun emitExactEvent(
        event: EcosEvent,
        listeners: EventsTypeListeners,
        isLocalEvent: Boolean,
        exclusive: Boolean = true,
        calledInTxn: Boolean = true
    ) {
        listeners.listeners.forEach { listener ->
            if (isLocalEvent) {
                triggerListener(listener, event, calledInTxn)
            } else {
                if (!listener.config.local && listener.config.exclusive == exclusive) {
                    triggerListener(listener, event, calledInTxn)
                }
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
            type = config.eventType,
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
            fullDataAtts.getAtts()
        )

        emitExactEvent(ecosEvent, typeListeners, true)
        return eventId
    }

    private fun getListenersForType(eventType: String): EventsTypeListeners? {

        val typeListeners = this.listenersContext.getListeners(eventType)

        if (typeListeners == null || typeListeners.listeners.isEmpty()) {
            log.debug { "Listeners doesn't found for type $eventType" }
            return null
        }
        return typeListeners
    }

    private fun triggerListener(listener: ListenerInfo, event: EcosEvent, calledInTxn: Boolean) {

        if (listener.config.filter !is VoidPredicate) {

            val resolvedFilter = recordsTemplateService.resolve(
                listener.config.filter,
                RecordRef.create("meta", "")
            )

            val element = RecordAttsElement.create(RecordAtts(RecordRef.EMPTY, event.attributes))
            if (!predicateService.isMatch(element, resolvedFilter)) {
                return
            }
        }

        val listenerAtts = ObjectData.create()
        listener.attributes.forEach { (alias, attribute) ->
            val value = event.attributes[attribute]
            listenerAtts[alias] = value
        }

        val convertedValue = when (val clazz = listener.config.dataClass) {
            RecordRef::class.java -> RecordRef.valueOf(event.attributes["rec?id"].asText())
            ObjectData::class.java -> listenerAtts
            Unit::class.java -> Unit
            EcosEvent::class.java -> event.withAttributes(listenerAtts)
            else -> dtoSchemaReader.instantiate(clazz, listenerAtts)
                ?: error("Event data can't be converted to $clazz. Data: $listenerAtts")
        }

        val action = listener.config.action
        if (listener.config.transactional) {
            if (calledInTxn) {
                action.accept(convertedValue)
            }
        } else {
            if (calledInTxn) {
                TxnContext.processListAfterCommit(
                    "events-after-commit",
                    { action.accept(convertedValue) }
                ) { elements -> elements.forEach {
                    try {
                        it.invoke()
                    } catch (e: Throwable) {
                        log.error(e) { "Error in after-commit event" }
                    }
                } }
            } else {
                action.accept(convertedValue)
            }
        }
    }

    override fun <T : Any> addListener(listener: ListenerConfig.Builder<T>.() -> Unit): ListenerHandle {
        val builder = ListenerConfig.create<T>()
        listener.invoke(builder)
        return addListener(builder.build())
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

    override fun getListeners(): Map<String, EventsTypeListeners> {
        val deepCopyOfListeners = mutableMapOf<String, EventsTypeListeners>()

        listenersContext.getListeners().forEach { (key, value) ->
            deepCopyOfListeners[key] = value.copy()
        }

        return deepCopyOfListeners
    }
}
