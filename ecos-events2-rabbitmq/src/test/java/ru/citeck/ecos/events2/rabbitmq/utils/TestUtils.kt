package ru.citeck.ecos.events2.rabbitmq.utils

import ru.citeck.ecos.events2.EventProperties
import ru.citeck.ecos.events2.EventService
import ru.citeck.ecos.events2.EventServiceFactory
import ru.citeck.ecos.events2.rabbitmq.RabbitMqEvents
import ru.citeck.ecos.events2.remote.RemoteEvents
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.records2.source.dao.local.RecordsDaoBuilder
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import kotlin.random.Random

class TestUtils {

    companion object {
        const val RECORD_SOURCE_TEMPLATE = "source_%s"

        fun createApp(
            name: String,
            rabbitConnection: RabbitMqConn,
            ecosZooKeeper: EcosZooKeeper,
            records: Map<String, Any>
        ): EventService {


            val recordsServiceFactory = RecordsServiceFactory()
            recordsServiceFactory.recordsServiceV1.register(
                RecordsDaoBuilder.create(RECORD_SOURCE_TEMPLATE.format(name))
                    .addRecords(records)
                    .build()
            )

            val serviceFactory = object : EventServiceFactory(recordsServiceFactory) {
                override fun createRemoteEvents(): RemoteEvents {
                    return RabbitMqEvents(this, rabbitConnection, ecosZooKeeper)
                }

                override fun createProperties(): EventProperties {
                    return EventProperties(appName = name, appInstanceId = name + "-" + Random.nextFloat())
                }
            }
            return serviceFactory.eventService
        }
    }

}