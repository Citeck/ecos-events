package ru.citeck.ecos.events2.rabbitmq.utils

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.rabbitmq.client.ConnectionFactory
import ecos.org.apache.curator.RetryPolicy
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.retry.RetryForever
import ecos.org.apache.curator.test.TestingServer
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.rabbitmq.RabbitMqEventsService
import ru.citeck.ecos.events2.remote.RemoteEventsService
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.records2.rest.RemoteRecordsRestApi
import ru.citeck.ecos.records2.source.dao.local.RecordsDaoBuilder
import ru.citeck.ecos.records3.RecordsProperties
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.resolver.RemoteRecordsResolver
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

class TestUtils {

    companion object {
        const val RECORD_SOURCE_TEMPLATE = "source_%s"

        private val recordsServices = mutableMapOf<String, RecordsServiceFactory>()

        fun createServers(): MockServers {

            val zkServer = TestingServer()

            val retryPolicy: RetryPolicy = RetryForever(7_000)

            val client = CuratorFrameworkFactory
                .newClient(zkServer.connectString, retryPolicy)
            client.start()
            val ecosZooKeeper = EcosZooKeeper(client).withNamespace("ecos")

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
                override fun createProperties(): RecordsProperties {
                    val props = RecordsProperties()
                    props.appName = appName
                    props.appInstanceId = appName + ":" + UUID.randomUUID()
                    return props
                }
                override fun createRemoteRecordsResolver(): RemoteRecordsResolver {
                    return RemoteRecordsResolver(
                        this,
                        object : RemoteRecordsRestApi {
                            override fun <T : Any> jsonPost(url: String, request: Any, respType: Class<T>): T {
                                val remoteAppName = url.substring(1).substringBefore('/')
                                val services = recordsServices[remoteAppName]!!
                                val restHandler = services.restHandlerAdapter
                                val urlPrefix = "/$remoteAppName"
                                val result = AtomicReference<Any>()
                                thread(start = true) {
                                    result.set(
                                        when (url) {
                                            urlPrefix + RemoteRecordsResolver.QUERY_URL -> restHandler.queryRecords(request)
                                            urlPrefix + RemoteRecordsResolver.MUTATE_URL -> restHandler.mutateRecords(request)
                                            urlPrefix + RemoteRecordsResolver.DELETE_URL -> restHandler.deleteRecords(request)
                                            urlPrefix + RemoteRecordsResolver.TXN_URL -> restHandler.txnAction(request)
                                            else -> error("Unknown url: '$url'")
                                        }
                                    )
                                }.join()
                                return Json.mapper.convert(result.get(), respType)!!
                            }
                        }
                    )
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