package ru.citeck.ecos.events2.rabbitmq.spring.config

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.citeck.ecos.events2.EventsProperties
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.rabbitmq.RabbitMqEventsService
import ru.citeck.ecos.events2.remote.RemoteEventsService
import ru.citeck.ecos.events2.type.RecordEventsService
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.rabbitmq.RabbitMqConnProvider
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import javax.annotation.PostConstruct

@Configuration
open class EventsServiceConfig(
    private val ecosZookeeper: EcosZooKeeper,
    private val rabbitMqConnProvider: RabbitMqConnProvider
) : EventsServiceFactory() {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    @PostConstruct
    override fun init() {
        super.init()
    }

    @Bean("eventsService")
    override fun createEventsService(): EventsService {
        log.info { "Event Service init" }
        return super.createEventsService()
    }

    override fun createProperties(): EventsProperties {
        val prop = EventsProperties()
        log.info("Event properties init: $prop")
        return prop
    }

    override fun createRemoteEvents(): RemoteEventsService? {
        val conn = rabbitMqConnProvider.getConnection()
        return RabbitMqEventsService(conn!!, this, ecosZookeeper)
    }

    @Bean
    override fun createRecordEventsService(): RecordEventsService {
        return super.createRecordEventsService()
    }

    @Autowired
    fun setRecordsServiceFactory(recordsServiceFactory: RecordsServiceFactory) {
        this.recordsServices = recordsServiceFactory
    }

    @Autowired
    fun setModelServiceFactory(modelServiceFactory: ModelServiceFactory) {
        this.modelServices = modelServiceFactory
    }
}