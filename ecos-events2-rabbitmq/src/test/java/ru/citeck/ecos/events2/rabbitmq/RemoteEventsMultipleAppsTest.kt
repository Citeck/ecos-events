package ru.citeck.ecos.events2.rabbitmq

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils.Companion.RECORD_SOURCE_TEMPLATE
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteEventsMultipleAppsTest {

    private lateinit var servers: TestUtils.MockServers

    private lateinit var eventService0: EventsService
    private lateinit var eventService1: EventsService
    private lateinit var eventService2: EventsService
    private lateinit var eventService3: EventsService

    private val testRecordRecordRef = RecordRef.create(RECORD_SOURCE_TEMPLATE.format("app1"), "rec")
        .toString()
    private val testRecord = TestRecord("abcdefg", 999)

    @BeforeAll
    fun setUp() {

        servers = TestUtils.createServers()

        eventService0 = TestUtils.createApp("app0", servers, emptyMap())
        eventService1 = TestUtils.createApp(
            "app1", servers,
            mapOf(
                Pair(testRecordRecordRef, testRecord)
            )
        )
        eventService2 = TestUtils.createApp("app2", servers, emptyMap())
        eventService3 = TestUtils.createApp("app3", servers, emptyMap())
    }

    @AfterAll
    fun tearDown() {
        servers.close()
    }

    @Test
    fun multipleAppsTest() {

        val data0 = ArrayList<DataClass>()
        val data1 = ArrayList<DataClass>()
        val data2 = ArrayList<TestRecordMetaWithEventData>()
        val dataWithRecordRef = ArrayList<DataClassWithRecordRef>()

        val testEventType = "test-event-type"

        eventService0.addListener(
            ListenerConfig.create<DataClass> {
                eventType = testEventType
                dataClass = DataClass::class.java
                withAction { evData ->
                    data0.add(evData)
                }
            }
        )

        eventService1.addListener(
            ListenerConfig.create<DataClass> {
                eventType = testEventType
                dataClass = DataClass::class.java
                withAction { evData ->
                    data1.add(evData)
                }
            }
        )

        eventService2.addListener(
            ListenerConfig.create<TestRecordMetaWithEventData> {
                eventType = testEventType
                dataClass = TestRecordMetaWithEventData::class.java
                withAction { evData ->
                    data2.add(evData)
                }
            }
        )

        eventService3.addListener(
            ListenerConfig.create<DataClassWithRecordRef> {
                eventType = testEventType
                dataClass = DataClassWithRecordRef::class.java
                withAction { evData ->
                    dataWithRecordRef.add(evData)
                }
            }
        )

        val emitter = eventService1.getEmitter<DataClass>(
            EmitterConfig.create {
                eventType = testEventType
                eventClass = DataClass::class.java
            }
        )

        val emitterWithRecordRef = eventService3.getEmitter<DataClassWithRecordRef>(
            EmitterConfig.create {
                eventType = testEventType
                eventClass = DataClassWithRecordRef::class.java
            }
        )

        val targetData = arrayListOf(
            DataClass("aa", "bb", testRecord),
            DataClass("cc", "dd", testRecord),
            DataClass("ee", "ff", testRecord)
        )

        val targetDataWithRecordRef = arrayListOf(
            DataClassWithRecordRef(RecordRef.valueOf("app0/ecos-config@test-config"), "some-str"),
            DataClassWithRecordRef(RecordRef.valueOf("app0/notification@temlate/email-template"), "some-str_0")
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

    private data class DataClassWithRecordRef(
        val record: RecordRef,
        val someStr: String
    )

    private data class DataClass(
        @AttName("field0") var field0: String?,
        @AttName("field1") var field1: String?,
        var rec: TestRecord
    )

    private data class TestRecord(
        var field0Str: String,
        var field1Num: Int
    )

    private data class TestRecordMetaWithEventData(
        @AttName("field0") var field0: String?,
        @AttName("field1") var field1: String?,

        @AttName("rec.field0Str")
        var field0Str: String?,
        @AttName("rec.field1Num")
        var field1Num: Int?
    )
}
