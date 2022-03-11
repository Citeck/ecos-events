package ru.citeck.ecos.events2.rabbitmq

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.listener.ListenerHandle
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.events2.type.RecordChangedEvent
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
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
    fun mergeTest() {

        val app0Events = app0.eventsService
        val app1Events = app1.eventsService

        val field0 = "FIELD_0"
        val field1 = "FIELD_1"

        val app1ListenedEvents = mutableListOf<EcosEvent>()

        var currentListener: ListenerHandle? = null
        val registerListener = { atts: Map<String, String> ->
            currentListener?.remove()
            currentListener = app1Events.addListener(ListenerConfig.create<EcosEvent> {
                withDataClass(EcosEvent::class.java)
                withEventType(RecordChangedEvent.TYPE)
                withAction {
                    app1ListenedEvents.add(it)
                }
                withAttributes(atts)
            })
            Thread.sleep(100)
        }

        registerListener(
            mapOf(
                "recId" to "record?id",
                "field0Before" to "before.$field0",
                "field0After" to "after.$field0",
                "field1Before" to "before.$field1",
                "field1After" to "after.$field1"
            )
        )

        val app0recordMutatedEmitter = app0Events.getEmitter(EmitterConfig.create<RecordChangedEvent> {
            withEventClass(RecordChangedEvent::class.java)
            withEventType(RecordChangedEvent.TYPE)
        })

        Thread.sleep(200)

        val doWithApp0Txn = { action: () -> Unit ->
            RequestContext.doWithCtx(app0.recordsServices) {
                RequestContext.doWithTxn {
                    action.invoke()
                }
            }
        }

        val fieldValues = mutableMapOf<String, String?>(
            field0 to null,
            field1 to null
        )
        val clearValues = {
            fieldValues[field0] = null
            fieldValues[field1] = null
            app1ListenedEvents.clear()
        }

        val createChangedEvent = { field: String,
                                   newValue: String ->

            val before = HashMap(fieldValues)
            fieldValues[field] = newValue
            val after = HashMap(fieldValues)

            RecordChangedEvent(
                RecordData("12345", fieldValues[field0], fieldValues[field1]),
                TypeInfo.create {
                    withModel(TypeModelDef.create {
                        withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId(field0)
                                    .withType(AttributeType.TEXT)
                                    .build(),
                                AttributeDef.create()
                                    .withId(field1)
                                    .withType(AttributeType.TEXT)
                                    .build()
                            )
                        )
                    })
                },
                before,
                after
            )
        }

        doWithApp0Txn {
            app0recordMutatedEmitter.emit(createChangedEvent(field0, "123"))
            app0recordMutatedEmitter.emit(createChangedEvent(field0, "123456"))
            app0recordMutatedEmitter.emit(createChangedEvent(field0, "123456789"))
        }

        Thread.sleep(200)

        assertThat(app1ListenedEvents).hasSize(1)

        assertThat(app1ListenedEvents[0].attributes.get("recId").asText()).isEqualTo("app0/@12345")
        assertThat(app1ListenedEvents[0].attributes.get("field0Before").asText()).isEqualTo("")
        assertThat(app1ListenedEvents[0].attributes.get("field0After").asText()).isEqualTo("123456789")

        clearValues()

        doWithApp0Txn {
            app0recordMutatedEmitter.emit(createChangedEvent(field0, "def"))
            app0recordMutatedEmitter.emit(createChangedEvent(field0, "ghi"))
            app0recordMutatedEmitter.emit(createChangedEvent(field0, "jkl"))
        }

        Thread.sleep(200)

        assertThat(app1ListenedEvents).hasSize(1)

        assertThat(app1ListenedEvents[0].attributes.get("recId").asText()).isEqualTo("app0/@12345")
        assertThat(app1ListenedEvents[0].attributes.get("field0Before").asText()).isEqualTo("")
        assertThat(app1ListenedEvents[0].attributes.get("field0After").asText()).isEqualTo("jkl")

        clearValues()

        registerListener(
            mapOf(
                "recId" to "record?id",
                "${field0}Before" to "before.$field0",
                "${field0}After" to "after.$field0",
                "${field1}Before" to "before.$field1",
                "${field1}After" to "after.$field1",
                "diff" to "diff.list[]{before[],after[],attId:def.id,attName:def.name?json,attType:def.type?str}"
            )
        )

        fieldValues[field0] = "before0"
        fieldValues[field1] = "before1"

        doWithApp0Txn {
            app0recordMutatedEmitter.emit(createChangedEvent(field0, "abc"))
            app0recordMutatedEmitter.emit(createChangedEvent(field1, "123"))
            app0recordMutatedEmitter.emit(createChangedEvent(field0, "def"))
            app0recordMutatedEmitter.emit(createChangedEvent(field1, "456"))
        }
        Thread.sleep(200)

        assertThat(app1ListenedEvents).hasSize(4)

        val assertTransition = { atts: ObjectData, att: String, before: String, after: String ->
            assertThat(atts.get("${att}Before").asText()).isEqualTo(before)
            assertThat(atts.get("${att}After").asText()).isEqualTo(after)

            val diff = atts.get("diff")
            assertThat(diff.size()).isEqualTo(1)
            assertThat(diff.get(0).get("attId").asText()).isEqualTo(att)
            assertThat(diff.get(0).get("attType").asText()).isEqualTo("TEXT")
            assertThat(diff.get(0).get("before")).isEqualTo(DataValue.create("""["$before"]"""))
            assertThat(diff.get(0).get("after")).isEqualTo(DataValue.create("""["$after"]"""))
        }

        app1ListenedEvents.forEachIndexed { idx, event ->
            val atts = event.attributes
            assertThat(atts.get("recId").asText()).isEqualTo("app0/@12345")
            when (idx) {
                0 -> assertTransition(atts, field0, "before0", "abc")
                1 -> assertTransition(atts, field1, "before1", "123")
                2 -> assertTransition(atts, field0, "abc", "def")
                3 -> assertTransition(atts, field1, "123", "456")
            }
        }
    }

    @Test
    fun baseTest() {

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

    data class RecordData(
        val id: String,
        val field0: String?,
        val field1: String?
    )
}