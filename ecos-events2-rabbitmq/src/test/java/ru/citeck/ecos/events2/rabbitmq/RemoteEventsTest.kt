package ru.citeck.ecos.events2.rabbitmq

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.rabbitmq.client.ConnectionFactory
import ecos.org.apache.curator.RetryPolicy
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.retry.RetryForever
import ecos.org.apache.curator.test.TestingServer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.events2.EventProperties
import ru.citeck.ecos.events2.EventService
import ru.citeck.ecos.events2.EventServiceFactory
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.remote.RemoteEvents
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.RecordsServiceFactory
import ru.citeck.ecos.records2.graphql.meta.annotation.MetaAtt
import ru.citeck.ecos.records2.source.dao.local.RecordsDaoBuilder
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import kotlin.random.Random
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteEventsTest {

    companion object {
        private val RECORD_SOURCE_TEMPLATE = "source_%s"
    }

    private var zkServer: TestingServer? = null

    lateinit var eventService0: EventService
    lateinit var eventService1: EventService
    lateinit var eventService2: EventService

    private val testRecordRecordRef = RecordRef.create(RECORD_SOURCE_TEMPLATE.format("app1"), "rec")
    private val testRecord = TestRecord("abcdefg", 999)

    @BeforeAll
    fun setUp() {
        zkServer = TestingServer()

        val retryPolicy: RetryPolicy = RetryForever(7_000)

        val client = CuratorFrameworkFactory
                .newClient(zkServer!!.connectString, retryPolicy)
        client.start()
        val ecosZooKeeper = EcosZooKeeper(client).withNamespace("ecos")

        val factory: ConnectionFactory = MockConnectionFactory()
        val connection = RabbitMqConn(factory)

        connection.waitUntilReady(5_000)

        eventService0 = createApp("app0", connection, ecosZooKeeper, emptyMap())
        eventService1 = createApp("app1", connection, ecosZooKeeper, mapOf(
                Pair(testRecordRecordRef, testRecord))
        )
        eventService2 = createApp("app2", connection, ecosZooKeeper, emptyMap())
    }

    @AfterAll
    fun tearDown() {
        zkServer?.stop()
    }

    @Test
    fun test() {

        val data0 = ArrayList<DataClass>()
        val data1 = ArrayList<DataClass>()
        val data2 = ArrayList<TestRecordMetaWithEventData>()

        val testEventType = "test-event-type"

        eventService0.addListener(ListenerConfig.create<DataClass> {
            eventType = testEventType
            dataClass = DataClass::class.java
            setAction { evData ->
                data0.add(evData)
            }
        })

        eventService1.addListener(ListenerConfig.create<DataClass> {
            eventType = testEventType
            dataClass = DataClass::class.java
            setAction { evData ->
                data1.add(evData)
            }
        })

        eventService2.addListener(ListenerConfig.create<TestRecordMetaWithEventData> {
            eventType = testEventType
            dataClass = TestRecordMetaWithEventData::class.java
            setAction { evData ->
                data2.add(evData)
            }
        })

        val emitter = eventService1.getEmitter<DataClass>(EmitterConfig.create {
            eventType = testEventType
            eventClass = DataClass::class.java
        })

        val targetData = arrayListOf(
            DataClass("aa", "bb", testRecord),
            DataClass("cc", "dd", testRecord),
            DataClass("ee", "ff", testRecord)
        )

        targetData.forEach { emitter.emit(it) }

        Thread.sleep(1000)

        assertEquals(targetData, data0)
        assertEquals(targetData, data1)

        assertEquals(testRecord.field0Str, data2[0].field0Str)
        assertEquals(testRecord.field1Num, data2[0].field1Num)
        assertEquals("aa", data2[0].field0)
        assertEquals("bb", data2[0].field1)
    }

    private fun createApp(name: String,
                          rabbitConnection: RabbitMqConn,
                          ecosZooKeeper: EcosZooKeeper,
                          records: Map<RecordRef, Any>) : EventService {

        val recordsServiceFactory = RecordsServiceFactory()
        recordsServiceFactory.recordsService.register(
                RecordsDaoBuilder.create(RECORD_SOURCE_TEMPLATE.format(name))
                        .addRecords(records)
                        .build()
        )

        val serviceFactory = object : EventServiceFactory(recordsServiceFactory) {
            override fun createRemoteEvents() : RemoteEvents {
                return RabbitMqEvents(this, rabbitConnection, ecosZooKeeper)
            }
            override fun createProperties(): EventProperties {
                return EventProperties(appName = name, appInstanceId = name + "-" + Random.nextFloat())
            }
        }
        return serviceFactory.eventService
    }

    data class DataClass(
        @MetaAtt("field0") var field0: String?,
        @MetaAtt("field1") var field1: String?,
        var rec: TestRecord
    )

    data class TestRecord(
        var field0Str: String,
        var field1Num: Int
    )

    data class TestRecordMetaWithEventData (
        @MetaAtt("field0") var field0: String?,
        @MetaAtt("field1") var field1: String?,

        @MetaAtt("rec.field0Str")
        var field0Str: String?,
        @MetaAtt("rec.field1Num")
        var field1Num: Int?
    )
}