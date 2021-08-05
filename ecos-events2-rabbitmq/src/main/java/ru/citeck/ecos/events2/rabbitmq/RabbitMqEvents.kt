package ru.citeck.ecos.events2.rabbitmq

import com.rabbitmq.client.BuiltinExchangeType
import mu.KotlinLogging
import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.EventServiceFactory
import ru.citeck.ecos.events2.remote.RemoteEvents
import ru.citeck.ecos.events2.remote.RemoteListener
import ru.citeck.ecos.rabbitmq.RabbitMqChannel
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import kotlin.collections.HashMap

class RabbitMqEvents(
    rabbitMqConnection: RabbitMqConn,
    private val factory: EventServiceFactory,
    private val ecosZooKeeper: EcosZooKeeper
) : RemoteEvents {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private var onRemoteListenersChanged: (String, List<RemoteListener>) -> Unit = { _, _ -> }
    private var eventListeners: Map<String, Set<String>> = emptyMap()

    private lateinit var outcomeChannel: RabbitMqChannel

    private val producedEventTypes: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap())
    private val remoteListeners: MutableMap<String, List<RemoteListener>> = ConcurrentHashMap()

    init {

        if (factory.properties.appName.isBlank()) {

            log.warn { "App name is blank. Consumers won't be registered" }

        } else {

            val eventsQueue = EventRabbitMqConstants.EVENTS_QUEUE_ID.format(factory.properties.appName)
            val initFlag = AtomicBoolean()

            repeat(factory.properties.concurrentEventConsumers) {
                rabbitMqConnection.doWithNewChannel(Consumer { channel ->

                    if (initFlag.compareAndSet(false, true)) {
                        outcomeChannel = channel
                        channel.declareExchange(eventsQueue, BuiltinExchangeType.DIRECT, true)
                        channel.declareQueue(eventsQueue, true)
                        channel.queueBind(eventsQueue, eventsQueue, eventsQueue)
                    }

                    channel.addConsumer(eventsQueue, EcosEvent::class.java) { event, _ ->
                        onEventReceived(event)
                    }
                })
            }
        }
    }

    override fun doWithListeners(action: (String, List<RemoteListener>) -> Unit) {
        this.onRemoteListenersChanged = action
    }

    override fun addProducedEventType(eventType: String) {
        if (producedEventTypes.add(eventType)) {
            updateRemoteListeners(eventType)
            ecosZooKeeper.watchChildrenRecursive("/events/${eventType}") { updateRemoteListeners(eventType) }
        }
    }

    private fun updateRemoteListeners(eventType: String) {

        val children = ecosZooKeeper.getChildren("/events/${eventType}")

        val listeners = mutableListOf<RemoteListener>()
        children.forEach {
            if (it != factory.properties.appName) {
                val listener = ecosZooKeeper.getValue("/events/${eventType}/${it}", RemoteListener::class.java)
                if (listener != null) {
                    listeners.add(listener)
                }
            }
        }
        if (listeners.isNotEmpty()) {
            remoteListeners[eventType] = listeners
            onRemoteListenersChanged.invoke(eventType, listeners)
        } else if (remoteListeners.contains(eventType)) {
            remoteListeners.remove(eventType)
            onRemoteListenersChanged.invoke(eventType, emptyList())
        }
    }

    override fun listenEvents(events: Map<String, Set<String>>) {

        if (eventListeners == events) {
            return
        }

        events.forEach { (k, v) ->
            val listener = RemoteListener(
                k,
                factory.properties.appName,
                factory.properties.appInstanceId,
                v
            )
            ecosZooKeeper.setValue("/events/${k}/${factory.properties.appName}", listener)
        }

        eventListeners.forEach { (k, _) ->
            if (events[k] == null) {
                ecosZooKeeper.setValue("/events/${k}/${factory.properties.appName}", null)
            }
        }

        eventListeners = HashMap(events)
    }

    override fun emitEvent(listener: RemoteListener, event: EcosEvent) {
        val queueName = EventRabbitMqConstants.EVENTS_QUEUE_ID.format(listener.appName)
        outcomeChannel.publishMsg(queueName, event)
    }

    fun onEventReceived(event: EcosEvent) {
        factory.eventService.emitRemoteEvent(event)
    }
}