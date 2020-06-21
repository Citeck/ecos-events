package ru.citeck.ecos.events.rabbitmq

import com.rabbitmq.client.BuiltinExchangeType
import mu.KotlinLogging
import ru.citeck.ecos.commons.rabbit.EcosRabbitConnection
import ru.citeck.ecos.events.EventServiceFactory
import ru.citeck.ecos.events.remote.RemoteEcosEvent
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

class RabbitMqEventsConsumer(eventServiceFactory: EventServiceFactory,
                             rabbitMqConnection: EcosRabbitConnection) {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private val properties = eventServiceFactory.properties
    private val eventService = eventServiceFactory.eventService
    private val listeners = eventServiceFactory.listenersContext

    init {

        if (properties.appName.isBlank()) {

            log.warn { "App name is blank. Consumers won't be registered" }

        } else {

            val eventsQueue = EventRabbitMqConstants.EVENTS_QUEUE_ID.format(properties.appName)
            val initFlag = AtomicBoolean()

            repeat(properties.concurrentEventConsumers) {
                rabbitMqConnection.doWithNewChannel(Consumer { channel ->

                    if (initFlag.compareAndSet(false, true)) {
                        channel.declareExchange(eventsQueue, BuiltinExchangeType.DIRECT, true)
                        channel.declareQueue(eventsQueue, true)
                        channel.queueBind(eventsQueue, eventsQueue, eventsQueue)
                    }

                    channel.addConsumer(eventsQueue, RemoteEcosEvent::class.java) { event, _ ->
                        onEventReceived(event)
                    }
                })
            }

            listeners.listenRemoteAtts { attsByType ->


            }

            /*eventService.addListener(ListenerConfig.create<ObjectData> {
                    dataClass = ObjectData::class.java
                    local = true
                    attributes = ...
                    eventType = "..."
                    setAction { data, event ->

                    }
                })*/
        }
    }

    fun onEventReceived(event: RemoteEcosEvent) {
        eventService.emitRemoteEvent(event.event, event.attributes)
    }
}