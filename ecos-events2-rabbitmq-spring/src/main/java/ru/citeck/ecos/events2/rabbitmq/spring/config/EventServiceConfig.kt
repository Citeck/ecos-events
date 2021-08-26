package ru.citeck.ecos.events2.rabbitmq.spring.config

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import ru.citeck.ecos.events2.EventProperties
import ru.citeck.ecos.events2.EventService
import ru.citeck.ecos.events2.EventServiceFactory
import ru.citeck.ecos.events2.rabbitmq.RabbitMqEvents
import ru.citeck.ecos.events2.remote.RemoteEvents
import ru.citeck.ecos.rabbitmq.RabbitMqConnProvider
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.zookeeper.EcosZooKeeper

@Configuration
@Profile("!test")
open class EventServiceConfig(
    private val ecosZookeeper: EcosZooKeeper,
    private val rabbitMqConnProvider: RabbitMqConnProvider,
    recordsServiceFactory: RecordsServiceFactory
) : EventServiceFactory(recordsServiceFactory) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    @Value("\${ecos.event.app.instance-id}")
    private lateinit var appInstanceId: String

    @Value("\${ecos.event.app.name}")
    private lateinit var appName: String

    @Value("\${ecos.event.concurrent-event-consumers:10}")
    private var concurrentEventConsumers: Int = 10

    @Bean
    override fun createEventService(): EventService {
        log.info { "Event Service init" }
        return super.createEventService()
    }

    override fun createProperties(): EventProperties {

        val prop = EventProperties(
            appInstanceId = appInstanceId,
            appName = appName,
            concurrentEventConsumers = concurrentEventConsumers
        )

        log.info("Event properties init: $prop")

        return prop
    }

    override fun createRemoteEvents(): RemoteEvents? {
        val conn = rabbitMqConnProvider.getConnection()
        return RabbitMqEvents(conn!!, this, ecosZookeeper)
    }

}