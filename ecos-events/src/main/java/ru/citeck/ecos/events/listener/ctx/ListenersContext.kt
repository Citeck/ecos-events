package ru.citeck.ecos.events.listener.ctx

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.events.EcosEvent
import ru.citeck.ecos.events.EventConstants
import ru.citeck.ecos.events.EventServiceFactory
import ru.citeck.ecos.events.listener.ListenerConfig
import ru.citeck.ecos.events.listener.ListenerHandle
import ru.citeck.ecos.events.remote.RemoteListener
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.PredicateUtils
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

class ListenersContext(serviceFactory: EventServiceFactory) {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private val recordsMetaService = serviceFactory.recordsServiceFactory.recordsMetaService

    private var listeners: Map<String, EventTypeListeners> = emptyMap()

    private val remoteAttsByType: MutableMap<String, Set<String>> = ConcurrentHashMap()
    private var rawListeners: List<ListenerConfig<*>> = ArrayList()

    private val remoteEvents = serviceFactory.remoteEvents
    private var remoteListeners: Map<String, List<RemoteListener>> = emptyMap()

    init {
        if (remoteEvents != null) {
            remoteEvents.doWithListeners { type, listeners -> setRemoteListeners(type, listeners) }
        } else {
            log.warn { "Remote events is null" }
        }
    }

    fun getListeners(type: String) : EventTypeListeners? {
        return listeners[type]
    }

    @Synchronized
    private fun update() {
        val currentRemoteAttsByType = HashMap(remoteAttsByType);
        initListeners()
        if (remoteEvents != null) {
            if (currentRemoteAttsByType != HashMap(remoteAttsByType)) {
                remoteEvents.listenEvents(remoteAttsByType)
            }
        }
    }

    private fun initListeners() {

        val newListeners = HashMap<String, EventTypeListeners>()

        val listenersByType = HashMap<String, MutableList<ListenerConfig<*>>>()
        rawListeners.forEach { listener ->
            listenersByType.computeIfAbsent(listener.eventType) { ArrayList() }.add(listener)
        }

        if (remoteEvents != null) {
            remoteListeners.values.flatten().map { listener ->
                ListenerConfig.create<EcosEvent> {
                    val atts = HashMap<String, String>()
                    listener.attributes.forEach { atts[it] = it }
                    attributes = atts
                    eventType = listener.eventType
                    setAction { event -> remoteEvents.emitEvent(listener, event) }
                    consistent = false
                    dataClass = EcosEvent::class.java
                    local = true
                }
            }.forEach { listener ->
                listenersByType.computeIfAbsent(listener.eventType) { ArrayList() }.add(listener)
            }
        }

        listenersByType.forEach { (type, listeners) ->

            val modelAtts = HashMap<String, String>()
            val recordAtts = HashSet<String>()
            val remoteAtts = HashSet<String>()

            val listenersInfo = ArrayList<ListenerInfo>()

            listeners.forEach { config ->

                val attributes = HashMap(getAttributesFromClass(config.dataClass))
                attributes.putAll(config.attributes)
                PredicateUtils.getAllPredicateAttributes(config.filter).forEach { att ->
                    attributes[EventConstants.FILTER_ATT_PREFIX + att] = att
                }

                attributes.values.forEach {
                    recordAtts.add(it);
                }

                if (!config.local) {
                    remoteAtts.addAll(attributes.values)
                }

                @Suppress("UNCHECKED_CAST")
                listenersInfo.add(ListenerInfo(attributes, config as ListenerConfig<Any>))
            }

            if (remoteAtts.isNotEmpty()) {
                this.remoteAttsByType[type] = remoteAtts
            }
            newListeners[type] = EventTypeListeners(recordAtts, listenersInfo)
        }

        this.listeners = newListeners
    }

    private fun getAttributesFromClass(clazz: Class<*>) : Map<String, String> {

        if (clazz != Unit::class.java
                && clazz != RecordRef::class.java
                && clazz != EcosEvent::class.java
                && clazz != MLText::class.java) {

            return recordsMetaService.getAttributes(clazz)
        }
        return emptyMap()
    }

    //todo: fix concurrent issues and remove synchronized

    @Synchronized
    fun removeListener(config: ListenerConfig<*>) {
        rawListeners = rawListeners.filter { it === config }
        update()
    }

    @Synchronized
    fun addListeners(listeners: List<ListenerConfig<*>>) : List<ListenerHandle> {
        val newListeners = ArrayList(rawListeners)
        newListeners.addAll(listeners)
        return setListeners(newListeners)
    }

    @Synchronized
    fun addListener(listener: ListenerConfig<*>) : ListenerHandle {
        return addListeners(listOf(listener))[0]
    }

    @Synchronized
    fun setListeners(listeners: List<ListenerConfig<*>>) : List<ListenerHandle> {
        rawListeners = ArrayList(listeners)
        update()
        return listeners.map { ListenerHandle(it, this) }
    }

    @Synchronized
    private fun setRemoteListeners(type: String, listeners: List<RemoteListener>) {
        val newValue = HashMap(remoteListeners)
        newValue[type] = listeners
        remoteListeners = newValue
        update()
    }
}