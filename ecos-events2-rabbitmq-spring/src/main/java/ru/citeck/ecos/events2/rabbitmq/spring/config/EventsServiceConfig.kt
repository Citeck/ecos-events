package ru.citeck.ecos.events2.rabbitmq.spring.config

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import ru.citeck.ecos.events2.EventsProperties
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.rabbitmq.RabbitMqEvents
import ru.citeck.ecos.events2.remote.RemoteEvents
import ru.citeck.ecos.rabbitmq.RabbitMqConnProvider
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import javax.annotation.PostConstruct

@Configuration
@Profile("!test")
open class EventsServiceConfig(
    private val ecosZookeeper: EcosZooKeeper,
    private val rabbitMqConnProvider: RabbitMqConnProvider
) : EventsServiceFactory() {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    @Value("\${ecos.event.concurrent-event-consumers:10}")
    private var concurrentEventConsumers: Int = 10

    @PostConstruct
    override fun init() {
        super.init()
    }

    @Bean
    override fun createEventsService(): EventsService {
        log.info { "Event Service init" }
        return super.createEventsService()
    }

    override fun createProperties(): EventsProperties {

        val prop = EventsProperties(
            concurrentEventConsumers = concurrentEventConsumers
        )

        log.info("Event properties init: $prop")

        return prop
    }

    override fun createRemoteEvents(): RemoteEvents? {
        val conn = rabbitMqConnProvider.getConnection()
        return RabbitMqEvents(conn!!, this, ecosZookeeper)
    }

    @Autowired
    fun setRecordsServiceFactory(recordsServiceFactory: RecordsServiceFactory) {
        this.recordsServices = recordsServiceFactory
    }
}