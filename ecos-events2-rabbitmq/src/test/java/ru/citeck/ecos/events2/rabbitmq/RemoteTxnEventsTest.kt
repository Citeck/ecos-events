package ru.citeck.ecos.events2.rabbitmq

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import ru.citeck.ecos.records3.record.dao.mutate.RecordsMutateDao
import ru.citeck.ecos.records3.record.request.RequestContext

class RemoteTxnEventsTest {

    private lateinit var servers: TestUtils.MockServers

    private lateinit var app0: EventsServiceFactory
    private lateinit var app1: EventsServiceFactory
    private lateinit var app2: EventsServiceFactory

    @BeforeEach
    fun before() {
        servers = TestUtils.createServers()

        app0 = TestUtils.createAppServices("app0", servers, emptyMap())
        app1 = TestUtils.createAppServices("app1", servers, emptyMap())
        app2 = TestUtils.createAppServices("app2", servers, emptyMap())
    }

    @Test
    fun test() {

        val app0Events = app0.eventsService
        val app1Events = app1.eventsService
        val app2Events = app2.eventsService

        val eventType = "test-event-type"

        val app0Emitter = app0Events.getEmitter(EmitterConfig.create<EventData> {
            withSource("app-0-test")
            withEventClass(EventData::class.java)
            withEventType(eventType)
        })

        val app1ListenedEvents = mutableListOf<EventData>()
        app1Events.addListener(ListenerConfig.create<EventData> {
            withDataClass(EventData::class.java)
            withEventType(eventType)
            withAction { app1ListenedEvents.add(it) }
        })

        Thread.sleep(200)

        app0Emitter.emit(EventData("test-data"))

        Thread.sleep(200)

        assertThat(app1ListenedEvents).hasSize(1)
        assertThat(app1ListenedEvents[0].field).isEqualTo("test-data")

        app1ListenedEvents.clear()

        RequestContext.doWithTxn {
            repeat(5) {
                app0Emitter.emit(EventData("test-data-$it"))
            }
            Thread.sleep(1000)
            assertThat(app1ListenedEvents).isEmpty()
        }
        Thread.sleep(100)

        assertThat(app1ListenedEvents).containsExactlyElementsOf((0 until 5).map {
            EventData("test-data-$it")
        })
        app1ListenedEvents.clear()

        app0.recordsServices.recordsServiceV1.register(object : RecordsMutateDao {
            override fun getId() = "test"
            override fun mutate(records: List<LocalRecordAtts>): List<String> {
                app0Emitter.emit(EventData("mutation!"))
                return records.map { it.id }
            }
        })

        val app0ListenedEvents = mutableListOf<EventData>()
        app0Events.addListener(ListenerConfig.create<EventData> {
            withDataClass(EventData::class.java)
            withEventType(eventType)
            withAction { app0ListenedEvents.add(it) }
        })
        val app2ListenedEvents = mutableListOf<EventData>()
        app2Events.addListener(ListenerConfig.create<EventData> {
            withDataClass(EventData::class.java)
            withEventType(eventType)
            withAction { app2ListenedEvents.add(it) }
        })
        Thread.sleep(100)

        val testRef = RecordRef.create(
            app0.recordsServices.properties.appName,
            "test",
            ""
        )
        app1.recordsServices.recordsServiceV1.mutateAtt(testRef, "att", "value")

        Thread.sleep(100)

        assertThat(app0ListenedEvents).containsExactly(EventData("mutation!"))
        assertThat(app1ListenedEvents).containsExactly(EventData("mutation!"))
        assertThat(app2ListenedEvents).containsExactly(EventData("mutation!"))

        app0ListenedEvents.clear()
        app1ListenedEvents.clear()
        app2ListenedEvents.clear()

        RequestContext.doWithTxn {
            repeat(2) {
                app1.recordsServices.recordsServiceV1.mutateAtt(testRef, "att", "value")
            }
            Thread.sleep(1000)
            assertThat(app0ListenedEvents).containsExactly(EventData("mutation!"), EventData("mutation!"))
            assertThat(app1ListenedEvents).containsExactly(EventData("mutation!"), EventData("mutation!"))
            assertThat(app2ListenedEvents).isEmpty()
        }

        Thread.sleep(200)

        assertThat(app2ListenedEvents).containsExactly(EventData("mutation!"), EventData("mutation!"))
    }

    @AfterEach
    fun after() {
        servers.close()
    }

    data class EventData(
        val field: String
    )
}