package ru.citeck.ecos.events2.rabbitmq

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestAppsCtx

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class InclusiveListenersTest {

    private lateinit var servers: TestAppsCtx
    private lateinit var app0Events: EventsService
    private lateinit var app10Events: EventsService
    private lateinit var app11Events: EventsService

    @BeforeEach
    fun setUp() {
        servers = TestAppsCtx()

        app0Events = servers.createApp("app0").eventsService

        app10Events = servers.createApp("app1").eventsService
        app11Events = servers.createApp("app1").eventsService
    }

    @Test
    fun notExclusiveListenersTest() {
        testImpl(false)
    }

    @Test
    fun exclusiveListenersTest() {
        testImpl(true)
    }

    private fun testImpl(exclusive: Boolean) {

        val app10EventsList = ArrayList<ObjectData>()
        val listenerHandle0 = app10Events.addListener(
            ListenerConfig.create<ObjectData> {
                withEventType("test-event")
                withDataClass(ObjectData::class.java)
                withAttributes(mapOf("att" to "att"))
                withAction { app10EventsList.add(it) }
                withExclusive(exclusive)
            }
        )

        val app11EventsList = ArrayList<ObjectData>()
        val listenerHandle1 = app11Events.addListener(
            ListenerConfig.create<ObjectData> {
                withEventType("test-event")
                withDataClass(ObjectData::class.java)
                withAttributes(mapOf("att" to "att"))
                withAction { app11EventsList.add(it) }
                withExclusive(exclusive)
            }
        )

        val app0TestEventEmitter = app0Events.getEmitter(
            EmitterConfig.create<ObjectData> {
                withEventType("test-event")
                withEventClass(ObjectData::class.java)
            }
        )

        Thread.sleep(200)

        val expectedEventData = ObjectData.create(
            """
            {
                "att": "value"
            }
            """.trimIndent()
        )

        app0TestEventEmitter.emit(expectedEventData)

        val checkEventsLists = {

            Thread.sleep(200)

            if (exclusive) {
                val (emptyList, notEmptyList) = if (app10EventsList.isEmpty()) {
                    app10EventsList to app11EventsList
                } else {
                    app11EventsList to app10EventsList
                }
                assertThat(emptyList).isEmpty()
                assertThat(notEmptyList).containsExactly(expectedEventData)
            } else {
                assertThat(app10EventsList).containsExactly(expectedEventData)
                assertThat(app11EventsList).containsExactly(expectedEventData)
            }
        }
        checkEventsLists()

        app10EventsList.clear()
        app11EventsList.clear()

        val app10TestEventEmitter = app10Events.getEmitter(
            EmitterConfig.create<ObjectData> {
                withEventType("test-event")
                withEventClass(ObjectData::class.java)
            }
        )

        app10TestEventEmitter.emit(expectedEventData)

        checkEventsLists()

        listenerHandle0.remove()
        listenerHandle1.remove()
    }
}
