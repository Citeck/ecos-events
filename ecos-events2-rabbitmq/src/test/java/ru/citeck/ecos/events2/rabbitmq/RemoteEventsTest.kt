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
import ru.citeck.ecos.events2.EventService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils.Companion.RECORD_SOURCE_TEMPLATE
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteEventsTest {

    private var zkServer: TestingServer? = null

    private lateinit var eventService0: EventService
    private lateinit var eventService1: EventService
    private lateinit var eventService2: EventService
    private lateinit var eventService3: EventService

    private val testRecordRecordRef = RecordRef.create(RECORD_SOURCE_TEMPLATE.format("app1"), "rec")
        .toString()
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

        eventService0 = TestUtils.createApp("app0", connection, ecosZooKeeper, emptyMap())
        eventService1 = TestUtils.createApp(
            "app1", connection, ecosZooKeeper, mapOf(
                Pair(testRecordRecordRef, testRecord)
            )
        )
        eventService2 = TestUtils.createApp("app2", connection, ecosZooKeeper, emptyMap())
        eventService3 = TestUtils.createApp("app3", connection, ecosZooKeeper, emptyMap())
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
        val dataWithRecordRef = ArrayList<DataClassWithRecordRef>()

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

        eventService3.addListener(ListenerConfig.create<DataClassWithRecordRef> {
            eventType = testEventType
            dataClass = DataClassWithRecordRef::class.java
            setAction { evData ->
                dataWithRecordRef.add(evData)
            }
        })

        val emitter = eventService1.getEmitter<DataClass>(EmitterConfig.create {
            eventType = testEventType
            eventClass = DataClass::class.java
        })

        val emitterWithRecordRef = eventService3.getEmitter<DataClassWithRecordRef>(EmitterConfig.create {
            eventType = testEventType
            eventClass = DataClassWithRecordRef::class.java
        })

        val targetData = arrayListOf(
            DataClass("aa", "bb", testRecord),
            DataClass("cc", "dd", testRecord),
            DataClass("ee", "ff", testRecord)
        )

        val targetDataWithRecordRef = arrayListOf(
            DataClassWithRecordRef(RecordRef.valueOf("ecos-config@test-config"), "some-str"),
            DataClassWithRecordRef(RecordRef.valueOf("notification@temlate/email-template"), "some-str_0")
        )

        targetData.forEach { emitter.emit(it) }

        targetDataWithRecordRef.forEach { emitterWithRecordRef.emit(it) }

        Thread.sleep(1000)

        assertEquals(targetData, data0)
        assertEquals(targetData, data1)

        assertEquals(testRecord.field0Str, data2[0].field0Str)
        assertEquals(testRecord.field1Num, data2[0].field1Num)
        assertEquals("aa", data2[0].field0)
        assertEquals("bb", data2[0].field1)

        assertEquals(targetDataWithRecordRef, dataWithRecordRef)
    }

    data class DataClassWithRecordRef(
        val record: RecordRef,
        val someStr: String
    )

    data class DataClass(
        @AttName("field0") var field0: String?,
        @AttName("field1") var field1: String?,
        var rec: TestRecord
    )

    data class TestRecord(
        var field0Str: String,
        var field1Num: Int
    )

    data class TestRecordMetaWithEventData(
        @AttName("field0") var field0: String?,
        @AttName("field1") var field1: String?,

        @AttName("rec.field0Str")
        var field0Str: String?,
        @AttName("rec.field1Num")
        var field1Num: Int?
    )
}