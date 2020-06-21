package ru.citeck.ecos.events.listener.ctx

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.events.EcosEvent
import ru.citeck.ecos.events.EventConstants
import ru.citeck.ecos.events.EventServiceFactory
import ru.citeck.ecos.events.listener.ListenerConfig
import ru.citeck.ecos.events.listener.ListenerHandler
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.PredicateUtils
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.HashMap
import kotlin.collections.HashSet

class ListenersContext(serviceFactory: EventServiceFactory) {

    private val recordsMetaService = serviceFactory.recordsServiceFactory.recordsMetaService

    private val listeners: MutableMap<String, EventTypeListeners> = ConcurrentHashMap()

    private val remoteAttsByType: MutableMap<String, Set<String>> = ConcurrentHashMap()
    private var remoteAttsListener: (Map<String, Set<String>>) -> Unit = {}

    private val rawListeners = ArrayList<ListenerConfig<*>>()

    private var isDirty = true

    fun getListeners(type: String) : EventTypeListeners? {
        update()
        return listeners[type]
    }

    fun listenRemoteAtts(listener: (Map<String, Set<String>>) -> Unit) {
        this.remoteAttsListener = listener
        update()
        listener.invoke(remoteAttsByType)
    }

    @Synchronized
    private fun update() {
        if (isDirty) {
            val currentRemoteAttsByType = HashMap(remoteAttsByType);
            initListeners()
            if (currentRemoteAttsByType != HashMap(remoteAttsByType)) {
                remoteAttsListener.invoke(remoteAttsByType)
            }
            isDirty = false
        }
    }

    private fun initListeners() {

        listeners.clear()
        remoteAttsByType.clear()

        val listenersByType = HashMap<String, MutableList<ListenerConfig<*>>>()
        rawListeners.forEach { listener ->
            listenersByType.computeIfAbsent(listener.eventType) { ArrayList() }.add(listener)
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
                    if (it.startsWith("$")
                            || it.startsWith(".att(n:\"$")
                            || it.startsWith(".atts(\"$")) {

                        modelAtts[it] = it.replaceFirst("$", "")
                    } else {
                        recordAtts.add(it);
                    }
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
            this.listeners[type] = EventTypeListeners(recordAtts, modelAtts, listenersInfo)
        }
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

    @Synchronized
    fun removeListener(config: ListenerConfig<*>) {
        rawListeners.removeIf { it === config }
        isDirty = true
    }

    @Synchronized
    fun addListeners(listeners: List<ListenerConfig<*>>) : List<ListenerHandler> {
        rawListeners.addAll(listeners)
        isDirty = true
        return listeners.map { ListenerHandler(it, this) }
    }

    @Synchronized
    fun addListener(listener: ListenerConfig<*>) : ListenerHandler {
        rawListeners.add(listener)
        isDirty = true
        return ListenerHandler(listener, this)
    }

    @Synchronized
    fun setListeners(listeners: List<ListenerConfig<*>>) : List<ListenerHandler> {
        rawListeners.clear()
        return addListeners(listeners)
    }
}