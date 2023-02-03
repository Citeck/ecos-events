package ru.citeck.ecos.events2.listener.ctx

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.listener.ListenerHandle
import ru.citeck.ecos.events2.remote.RemoteAppEventListener
import ru.citeck.ecos.events2.remote.RemoteEventListenerData
import ru.citeck.ecos.events2.remote.RemoteEventListenerKey
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.OrPredicate
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

class ListenersContext(serviceFactory: EventsServiceFactory) {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private val dtoSchemaReader = serviceFactory.recordsServices.dtoSchemaReader
    private val attSchemaWriter = serviceFactory.recordsServices.attSchemaWriter

    private var listeners: Map<String, EventsTypeListeners> = emptyMap()

    private val remoteEvents = serviceFactory.remoteEventsService

    private var rawListeners: Set<ListenerConfig<*>> = emptySet()

    private var listenersToRemote: Map<RemoteEventListenerKey, RemoteEventListenerData> = emptyMap()
    private var listenersFromRemote: Map<String, List<RemoteAppEventListener>> = emptyMap()

    private var listenersToRemoteInitialized = false

    init {
        if (remoteEvents != null) {
            remoteEvents.listenListenersChange { type, listeners ->
                setRemoteListeners(type, listeners)
            }
        } else {
            log.warn { "Remote events is null" }
        }
    }

    fun getListeners(): Map<String, EventsTypeListeners> {
        return listeners
    }

    fun getListeners(type: String): EventsTypeListeners? {
        return listeners[type]
    }

    @Synchronized
    fun update() {
        val newListenersToRemote = hashMapOf<RemoteEventListenerKey, RemoteEventListenerData>()
        initListeners(newListenersToRemote)
        if (remoteEvents != null) {
            if (!listenersToRemoteInitialized || newListenersToRemote != HashMap(listenersToRemote)) {
                this.listenersToRemote = newListenersToRemote
                remoteEvents.listenEventsFromRemote(newListenersToRemote)
                listenersToRemoteInitialized = true
            }
        }
    }

    private fun initListeners(listenersToRemote: MutableMap<RemoteEventListenerKey, RemoteEventListenerData>) {

        val newListeners = HashMap<String, EventsTypeListeners>()

        val listenersByType = HashMap<String, MutableList<ListenerConfig<*>>>()
        rawListeners.forEach { listener ->
            listenersByType.computeIfAbsent(listener.eventType) { ArrayList() }.add(listener)
        }

        if (remoteEvents != null) {
            listenersFromRemote.forEach { (eventType, listeners) ->
                for (listener in listeners) {

                    val atts = HashMap<String, String>()
                    listener.attributes.forEach { atts[it] = it }

                    val listenerConfig = ListenerConfig.create<EcosEvent> {
                        withAttributes(atts)
                        withEventType(eventType)
                        withTransactional(listener.transactional)
                        withAction { event ->
                            remoteEvents.emitEvent(listener.targetAppKey, event, listener.transactional)
                        }
                        withFilter(listener.filter)
                        withDataClass(EcosEvent::class.java)
                        withLocal(true)
                    }
                    listenersByType.computeIfAbsent(listenerConfig.eventType) { ArrayList() }.add(listenerConfig)
                }
            }
        }

        listenersByType.forEach { (type, listeners) ->

            val recordAtts = HashSet<String>()

            val exclusiveRemoteAtts = HashSet<String>()
            val exclusiveFilter = mutableListOf<Predicate>()
            val inclusiveRemoteAtts = HashSet<String>()
            val inclusiveFilter = mutableListOf<Predicate>()

            var exclusiveTransactionalListener = false

            val listenersInfo = ArrayList<ListenerInfo>()

            listeners.forEach { config ->

                val attributes = HashMap(getAttributesFromClass(config.dataClass))
                attributes.putAll(config.attributes)

                val attsToLoad = HashSet<String>(attributes.values)
                attsToLoad.addAll(PredicateUtils.getAllPredicateAttributes(config.filter))

                recordAtts.addAll(attsToLoad)

                if (!config.local) {
                    if (config.exclusive) {
                        exclusiveRemoteAtts.addAll(attsToLoad)
                        exclusiveFilter.add(config.filter)
                        if (config.transactional) {
                            exclusiveTransactionalListener = true
                        }
                    } else {
                        inclusiveRemoteAtts.addAll(attsToLoad)
                        inclusiveFilter.add(config.filter)
                    }
                }

                @Suppress("UNCHECKED_CAST")
                listenersInfo.add(ListenerInfo(attributes, config as ListenerConfig<Any>))
            }
            if (exclusiveRemoteAtts.isNotEmpty()) {
                listenersToRemote[RemoteEventListenerKey(type, true)] = RemoteEventListenerData(
                    exclusiveRemoteAtts,
                    createRemoteFilter(exclusiveFilter),
                    exclusiveTransactionalListener
                )
            }
            if (inclusiveRemoteAtts.isNotEmpty()) {
                listenersToRemote[RemoteEventListenerKey(type, false)] = RemoteEventListenerData(
                    inclusiveRemoteAtts,
                    createRemoteFilter(inclusiveFilter)
                )
            }
            newListeners[type] = EventsTypeListeners(recordAtts, listenersInfo)
        }
        this.listeners = newListeners
    }

    private fun createRemoteFilter(filters: List<Predicate>): Predicate {
        if (filters.isEmpty() || filters.any { it is VoidPredicate }) {
            return VoidPredicate.INSTANCE
        }
        return OrPredicate.of(filters)
    }

    private fun getAttributesFromClass(clazz: Class<*>): Map<String, String> {

        if (clazz.isAssignableFrom(Map::class.java)) {
            return emptyMap()
        }

        if (clazz == RecordRef::class.java) {
            return mapOf("rec?id" to "rec?id")
        }

        if (clazz != Unit::class.java &&
            clazz != EcosEvent::class.java &&
            clazz != MLText::class.java &&
            clazz != ObjectData::class.java &&
            clazz != DataValue::class.java
        ) {

            val attsSchema = dtoSchemaReader.read(clazz)

            return attSchemaWriter.writeToMap(attsSchema)
        }
        return emptyMap()
    }

    @Synchronized
    fun removeListener(config: ListenerConfig<*>) {
        rawListeners = rawListeners.filter { it != config }.toSet()
        update()
    }

    @Synchronized
    fun removeListener(id: String) {
        rawListeners = rawListeners.filter { it.id != id }.toSet()
        update()
    }

    @Synchronized
    fun addListeners(listeners: List<ListenerConfig<*>>): List<ListenerHandle> {
        val newListeners = ArrayList(rawListeners)
        newListeners.addAll(listeners)
        return setListeners(newListeners)
    }

    @Synchronized
    fun addListener(listener: ListenerConfig<*>): ListenerHandle {
        val newListeners = addListeners(listOf(listener))
        return newListeners.first { it.config.id == listener.id }
    }

    @Synchronized
    fun setListeners(listeners: List<ListenerConfig<*>>): List<ListenerHandle> {
        rawListeners = ArrayList(listeners).toSet()
        update()
        return listeners.map { ListenerHandle(it, this) }
    }

    @Synchronized
    private fun setRemoteListeners(type: String, listeners: List<RemoteAppEventListener>) {
        val newValue = HashMap(listenersFromRemote)
        newValue[type] = listeners
        listenersFromRemote = newValue
        update()
    }
}
