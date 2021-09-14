package ru.citeck.ecos.events2.rabbitmq

import com.rabbitmq.client.BuiltinExchangeType
import mu.KotlinLogging
import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.remote.RemoteEvents
import ru.citeck.ecos.events2.remote.RemoteListener
import ru.citeck.ecos.rabbitmq.RabbitMqChannel
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class RabbitMqEvents(
    rabbitMqConnection: RabbitMqConn,
    private val factory: EventsServiceFactory,
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

    private val appName: String
    private val appInstanceId: String

    init {

        val recProps = factory.recordsServices.properties
        appName = recProps.appName
        appInstanceId = recProps.appInstanceId

        if (appName.isBlank()) {

            log.warn { "App name is blank. Consumers won't be registered" }

        } else {

            val eventsQueue = EventRabbitMqConstants.EVENTS_QUEUE_ID.format(appName)
            val initFlag = AtomicBoolean()

            repeat(factory.properties.concurrentEventConsumers) {
                rabbitMqConnection.doWithNewChannel { channel ->

                    if (initFlag.compareAndSet(false, true)) {
                        outcomeChannel = channel
                        channel.declareExchange(eventsQueue, BuiltinExchangeType.DIRECT, true)
                        channel.declareQueue(eventsQueue, true)
                        channel.queueBind(eventsQueue, eventsQueue, eventsQueue)
                    }

                    channel.addConsumer(eventsQueue, EcosEvent::class.java) { event, _ ->
                        onEventReceived(event)
                    }
                }
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
            if (it != appName) {
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
                appName,
                appInstanceId,
                v
            )
            ecosZooKeeper.setValue("/events/${k}/$appName", listener)
        }

        eventListeners.forEach { (k, _) ->
            if (events[k] == null) {
                ecosZooKeeper.setValue("/events/${k}/$appName", null)
            }
        }

        eventListeners = HashMap(events)
    }

    override fun emitEvent(target: String, event: EcosEvent) {
        val queueName = EventRabbitMqConstants.EVENTS_QUEUE_ID.format(target)
        outcomeChannel.publishMsg(queueName, event)
    }

    fun onEventReceived(event: EcosEvent) {
        factory.eventService.emitRemoteEvent(event)
    }
}