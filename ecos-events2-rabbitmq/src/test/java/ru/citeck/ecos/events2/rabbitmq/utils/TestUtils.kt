package ru.citeck.ecos.events2.rabbitmq.utils

import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.rabbitmq.RabbitMqEventsService
import ru.citeck.ecos.events2.remote.RemoteEventsService
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.rabbitmq.test.EcosRabbitMqTest
import ru.citeck.ecos.records2.source.dao.local.RecordsDaoBuilder
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.resolver.RemoteRecordsResolver
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import ru.citeck.ecos.zookeeper.test.EcosZooKeeperTest
import java.util.*

class TestUtils {

    companion object {
        const val RECORD_SOURCE_TEMPLATE = "source_%s"

        private val recordsServices = mutableMapOf<String, RecordsServiceFactory>()

        fun createServers(): MockServers {
            return MockServers(
                EcosZooKeeperTest.createZooKeeper().withNamespace("ecos"),
                EcosRabbitMqTest.createConnection()
            )
        }

        fun createApp(
            name: String,
            servers: MockServers,
            records: Map<String, Any>
        ): EventsService {
            return createAppServices(name, servers, records).eventsService
        }

        fun createAppServices(
            name: String,
            servers: MockServers,
            records: Map<String, Any>
        ): EventsServiceFactory {

            val recordsServiceFactory = createRecordsServices(name)

            recordsServiceFactory.recordsServiceV1.register(
                RecordsDaoBuilder.create(RECORD_SOURCE_TEMPLATE.format(name))
                    .addRecords(records)
                    .build()
            )

            val serviceFactory = object : EventsServiceFactory() {
                override fun createRemoteEvents(): RemoteEventsService {
                    return RabbitMqEventsService(servers.rabbitmq, this, servers.zookeeper, EcosWebAppApiMock())
                }
            }
            serviceFactory.recordsServices = recordsServiceFactory
            serviceFactory.init()

            return serviceFactory
        }

        private fun createRecordsServices(appName: String): RecordsServiceFactory {

            val services = object : RecordsServiceFactory() {
                override fun getEcosWebAppApi(): EcosWebAppApi {
                    val ctx = EcosWebAppApiMock(appName, appName + ":" + UUID.randomUUID())
                    ctx.webClientExecuteImpl = { appName, path, body ->
                        val services = recordsServices[appName]!!
                        val restHandler = services.restHandlerAdapter
                        when (path) {
                            RemoteRecordsResolver.QUERY_PATH -> restHandler.queryRecords(body)
                            RemoteRecordsResolver.MUTATE_PATH -> restHandler.mutateRecords(body)
                            RemoteRecordsResolver.DELETE_PATH -> restHandler.deleteRecords(body)
                            RemoteRecordsResolver.TXN_PATH -> restHandler.txnAction(body)
                            else -> error("Unknown path: '$path'")
                        }
                    }
                    return ctx
                }
            }
            recordsServices[appName] = services
            return services
        }
    }

    class MockServers(
        val zookeeper: EcosZooKeeper,
        val rabbitmq: RabbitMqConn
    )
}
