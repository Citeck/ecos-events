package ru.citeck.ecos.events2

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.emitter.EventsEmitter
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.listener.ListenerHandle
import ru.citeck.ecos.events2.listener.ctx.EventsTypeListeners
import ru.citeck.ecos.events2.listener.ctx.ListenerInfo
import ru.citeck.ecos.events2.listener.ctx.ListenersContext
import ru.citeck.ecos.events2.obs.EventsObsCtx
import ru.citeck.ecos.micrometer.obs.EcosObs
import ru.citeck.ecos.records2.predicate.element.elematts.RecordAttsElement
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.system.measureTimeMillis

class EventsServiceImpl(serviceFactory: EventsServiceFactory) : EventsService {

    companion object {
        const val EVENT_ATTR = "event"

        private val log = KotlinLogging.logger {}
    }

    private val micrometerContext = serviceFactory.recordsServices.micrometerContext

    private val remoteEvents = serviceFactory.remoteEventsService
    private val recordsTemplateService = serviceFactory.recordsServices.recordsTemplateService

    private val emitters: MutableMap<EmitterConfig<*>, EventsEmitter<*>> = ConcurrentHashMap()

    private val predicateService = serviceFactory.recordsServices.predicateService
    private val recordsService = serviceFactory.recordsServices.recordsService
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

    override fun emitEventFromRemote(event: EcosEvent, exclusive: Boolean, calledInSameTxn: Boolean) {

        micrometerContext.createObs(EventsObsCtx.RemoteDispatch(event, exclusive, calledInSameTxn))
            .observe {
                val typeListeners = getListenersForType(event.type) ?: return@observe
                emitExactEvent(event, typeListeners, false, exclusive, calledInSameTxn)
            }
    }

    private fun emitExactEvent(
        event: EcosEvent,
        listeners: EventsTypeListeners,
        isLocalEvent: Boolean,
        exclusive: Boolean = true,
        calledInSameTxn: Boolean = true
    ) {
        for (listener in listeners.listeners) {
            if (listener.config.transactional && !calledInSameTxn) {
                continue
            }
            if (isLocalEvent) {
                triggerListener(listener, event, calledInSameTxn)
            } else {
                if (!listener.config.transactional && calledInSameTxn) {
                    continue
                }
                if (!listener.config.local && listener.config.exclusive == exclusive) {
                    triggerListener(listener, event, calledInSameTxn)
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

    private fun createListenerObs(listener: ListenerInfo, event: EcosEvent, phase: EventsObsCtx.Phase): EcosObs {

        return micrometerContext.createObs(EventsObsCtx.Listener(event, listener, phase))
    }

    private fun triggerListener(listener: ListenerInfo, event: EcosEvent, calledInSameTxn: Boolean) {

        createListenerObs(listener, event, EventsObsCtx.Phase.PREPARE)
            .observe { triggerListenerImpl(listener, event, calledInSameTxn) }
    }

    private fun triggerListenerImpl(listener: ListenerInfo, event: EcosEvent, calledInSameTxn: Boolean) {

        val triggerTime = measureTimeMillis {
            val matches = createListenerObs(listener, event, EventsObsCtx.Phase.FILTER)
                .observe {
                    if (listener.config.filter is VoidPredicate) {
                        true
                    } else {
                        val resolvedFilter = recordsTemplateService.resolve(
                            listener.config.filter,
                            EntityRef.create("meta", "")
                        )
                        val element = RecordAttsElement.create(RecordAtts(EntityRef.EMPTY, event.attributes))
                        predicateService.isMatch(element, resolvedFilter)
                    }
                }
            if (!matches) {
                return
            }

            val convertedValue = createListenerObs(listener, event, EventsObsCtx.Phase.CONVERT)
                .observe {
                    val listenerAtts = ObjectData.create()
                    listener.attributes.forEach { (alias, attribute) ->
                        listenerAtts[alias] = event.attributes[attribute]
                    }
                    when (val clazz = listener.config.dataClass) {
                        EntityRef::class.java ->
                            EntityRef.valueOf(event.attributes[ListenersContext.ENTITY_REF_ID_ATT].asText())
                        ObjectData::class.java -> listenerAtts
                        Unit::class.java -> Unit
                        EcosEvent::class.java -> event.withAttributes(listenerAtts)
                        else -> dtoSchemaReader.instantiate(clazz, listenerAtts)
                            ?: error("Event data can't be converted to $clazz. Data: $listenerAtts")
                    }
                }

            val action: (Any) -> Unit = { value ->
                createListenerObs(listener, event, EventsObsCtx.Phase.ACTION)
                    .observe { listener.config.action.accept(value) }
            }
            if (listener.config.transactional) {
                if (calledInSameTxn) {
                    action(convertedValue)
                }
            } else {
                if (calledInSameTxn) {
                    TxnContext.processListAfterCommit(
                        "events-after-commit",
                        { action(convertedValue) }
                    ) { elements ->

                        val afterCommitTime = measureTimeMillis {
                            elements.forEach {
                                try {
                                    it.invoke()
                                } catch (e: Throwable) {
                                    log.error(e) { "Error in after-commit event ${event.id} with type ${event.type}" }
                                }
                            }
                        }

                        log.debug {
                            "Trigger listeners after commit for event ${event.id} " +
                                "with type ${event.type} in $afterCommitTime ms"
                        }
                    }
                } else {
                    action(convertedValue)
                }
            }
        }

        log.debug {
            "Triggered listener ${listener.config.id} for event ${event.id} " +
                "with type ${event.type} in $triggerTime ms"
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
