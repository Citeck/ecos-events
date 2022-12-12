package ru.citeck.ecos.events2.rabbitmq.utils

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.rabbitmq.client.ConnectionFactory
import ecos.org.apache.curator.test.TestingServer
import ru.citeck.ecos.commons.test.EcosWebAppApiMock
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.rabbitmq.RabbitMqEventsService
import ru.citeck.ecos.events2.remote.RemoteEventsService
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.records2.source.dao.local.RecordsDaoBuilder
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.resolver.RemoteRecordsResolver
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import java.util.*

class TestUtils {

    companion object {
        const val RECORD_SOURCE_TEMPLATE = "source_%s"

        private val recordsServices = mutableMapOf<String, RecordsServiceFactory>()

        fun createServers(): MockServers {

            val zkServer = TestingServer()
            zkServer.start()
            val ecosZooKeeper = EcosZooKeeper(zkServer.connectString).withNamespace("ecos")

            val factory: ConnectionFactory = MockConnectionFactory()
            val rabbitMqConn = RabbitMqConn(factory)

            rabbitMqConn.waitUntilReady(5_000)

            return MockServers(zkServer, ecosZooKeeper, rabbitMqConn)
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
                    return RabbitMqEventsService(servers.rabbitmq, this, servers.zookeeper)
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
        val zkServer: TestingServer,
        val zookeeper: EcosZooKeeper,
        val rabbitmq: RabbitMqConn
    ) {
        fun close() {
            zkServer.stop()
            rabbitmq.close()
        }
    }
}
