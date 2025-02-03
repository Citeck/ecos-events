package ru.citeck.ecos.events2.rabbitmq.utils

import com.fasterxml.jackson.databind.node.ObjectNode
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.rabbitmq.RabbitMqEventsService
import ru.citeck.ecos.events2.remote.RemoteEventsService
import ru.citeck.ecos.events2.web.EmitEventWebExecutor
import ru.citeck.ecos.rabbitmq.test.EcosRabbitMqTest
import ru.citeck.ecos.records2.source.dao.local.InMemRecordsDao
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.resolver.RemoteRecordsResolver
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.web.EcosWebHeaders
import ru.citeck.ecos.webapp.api.web.body.BodyContentType
import ru.citeck.ecos.webapp.api.web.body.EcosWebBodyReader
import ru.citeck.ecos.webapp.api.web.body.EcosWebBodyWriter
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorReq
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorResp
import ru.citeck.ecos.zookeeper.test.EcosZooKeeperTest
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

class TestAppsCtx {

    companion object {
        const val RECORD_SOURCE_TEMPLATE = "source_%s"

        private val instanceIdCounters = ConcurrentHashMap<String, AtomicInteger>()
    }

    val zookeeper = EcosZooKeeperTest.createZooKeeper().withNamespace("ecos")
    val rabbitmq = EcosRabbitMqTest.createConnection()

    private val appsByName = ConcurrentHashMap<String, MutableList<TestApp>>()

    private fun execWebApiRequest(appName: String, path: String, body: Any): Any {
        val targetApps = appsByName[appName]
        if (targetApps.isNullOrEmpty()) {
            error("Target app $appName is not registered")
        }
        val targetApp = targetApps.random()
        val recordsRest = targetApp.recordsServices.restHandlerAdapter
        val bodyNode = Json.mapper.convertNotNull(body, ObjectNode::class.java)

        val webApiRequest = WebApiTestRequest(Json.mapper.toBytesNotNull(body))
        val webApiResponse = WebApiTestResponse()

        return when (path) {
            RemoteRecordsResolver.QUERY_PATH -> recordsRest.queryRecords(bodyNode, 2)
            RemoteRecordsResolver.MUTATE_PATH -> recordsRest.mutateRecords(bodyNode, 1)
            RemoteRecordsResolver.DELETE_PATH -> recordsRest.deleteRecords(bodyNode, 1)
            EmitEventWebExecutor.PATH -> targetApp.eventsEmitWebExecutor.execute(webApiRequest, webApiResponse)
            else -> error("Unknown path: '$path'")
        }
    }

    fun createApp(
        appName: String
    ): TestApp {
        val instanceIdx = instanceIdCounters.computeIfAbsent(appName) { AtomicInteger() }.getAndIncrement()
        val app = TestApp(appName, "i-$instanceIdx")
        appsByName.computeIfAbsent(appName) { CopyOnWriteArrayList() }.add(app)
        return app
    }

    inner class TestApp(
        val appName: String,
        val appInstanceId: String
    ) {

        val webAppApi = EcosWebAppApiMock(appName, appInstanceId)

        val eventsServices: EventsServiceFactory
        val eventsService: EventsService
            get() = eventsServices.eventsService
        val eventsEmitWebExecutor: EmitEventWebExecutor

        val recordsServices: RecordsServiceFactory
        val recordsService: RecordsService
            get() = recordsServices.recordsService

        private val recordsDao = InMemRecordsDao<Any>(RECORD_SOURCE_TEMPLATE.format(appName))

        init {
            webAppApi.webClientExecuteImpl = { appName, path, body ->
                execWebApiRequest(appName, path, body)
            }
            recordsServices = object : RecordsServiceFactory() {
                override fun getEcosWebAppApi(): EcosWebAppApi {
                    return webAppApi
                }
            }
            recordsServices.recordsService.register(recordsDao)

            eventsServices = object : EventsServiceFactory() {
                override fun createRemoteEvents(): RemoteEventsService {
                    return RabbitMqEventsService(rabbitmq, this, zookeeper, webAppApi)
                }
            }
            eventsServices.recordsServices = recordsServices
            eventsServices.init()
            eventsEmitWebExecutor = EmitEventWebExecutor(eventsService)
        }

        fun registerRecord(id: String, value: Any) {
            if (id.contains(EntityRef.SOURCE_ID_DELIMITER) ||
                id.contains(EntityRef.APP_NAME_DELIMITER) ||
                id.isBlank()
            ) {
                error("Invalid record id: '$id'")
            }
            recordsDao.setRecord(id, value)
        }

        fun registerRecords(records: Map<String, Any>) {
            records.forEach { (k, v) -> registerRecord(k, v) }
        }
    }

    private class WebApiTestRequest(val body: ByteArray) : EcosWebExecutorReq {

        override fun getApiVersion(): Int {
            return 0
        }

        override fun getBodyReader(): EcosWebBodyReader {
            return object : EcosWebBodyReader {
                override fun getType(): BodyContentType {
                    return BodyContentType.JSON
                }

                override fun getInputStream(): InputStream {
                    return ByteArrayInputStream(body)
                }

                override fun <T : Any> readDto(type: Class<out T>): T {
                    return Json.mapper.readNotNull(body, type)
                }

                override fun <T : Any> readDtoOrNull(type: Class<out T>): T? {
                    return Json.mapper.read(body, type)
                }
            }
        }

        override fun getHeaders(): EcosWebHeaders {
            error("Not implemented")
        }
    }

    private class WebApiTestResponse : EcosWebExecutorResp {
        override fun setHeader(key: String, value: Any?) {
        }

        override fun getBodyWriter(): EcosWebBodyWriter {
            error("Not implemented")
        }
    }
}
