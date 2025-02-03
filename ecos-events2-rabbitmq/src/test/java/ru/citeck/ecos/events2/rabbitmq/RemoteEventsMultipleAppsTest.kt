package ru.citeck.ecos.events2.rabbitmq

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestAppsCtx
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.webapp.api.entity.EntityRef

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteEventsMultipleAppsTest {

    private lateinit var servers: TestAppsCtx

    private lateinit var eventService0: EventsService
    private lateinit var eventService1: EventsService
    private lateinit var eventService2: EventsService
    private lateinit var eventService3: EventsService

    private val testRecord = TestRecord("abcdefg", 999)

    @BeforeAll
    fun setUp() {

        servers = TestAppsCtx()

        eventService0 = servers.createApp("app0").eventsService

        val app1 = servers.createApp("app1")
        app1.registerRecord("rec", testRecord)
        eventService1 = app1.eventsService

        eventService2 = servers.createApp("app2").eventsService
        eventService3 = servers.createApp("app3").eventsService
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
            DataClassWithRecordRef(EntityRef.valueOf("app0/ecos-config@test-config"), "some-str"),
            DataClassWithRecordRef(EntityRef.valueOf("app0/notification@temlate/email-template"), "some-str_0")
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
        val record: EntityRef,
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
