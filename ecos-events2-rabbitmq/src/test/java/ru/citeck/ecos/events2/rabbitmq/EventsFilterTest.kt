package ru.citeck.ecos.events2.rabbitmq

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.records2.predicate.model.Predicates

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class EventsFilterTest {

    private lateinit var servers: TestUtils.MockServers
    private lateinit var app0Events: EventsService
    private lateinit var app1Events: EventsService

    @BeforeEach
    fun setUp() {
        servers = TestUtils.createServers()

        app0Events = TestUtils.createApp("app0", servers, emptyMap())
        app1Events = TestUtils.createApp("app1", servers, emptyMap())
    }

    @Test
    fun eventsFilterNotExclusiveTest() {
        testImpl(false)
    }

    @Test
    fun eventsFilterExclusiveTest() {
        testImpl(true)
    }

    private fun testImpl(exclusive: Boolean) {

        val baseListenerConfig = ListenerConfig.create<ObjectData> {
            withEventType("test-event")
            withDataClass(ObjectData::class.java)
            withAttributes(mapOf("name" to "name"))
            withAction {}
            withExclusive(exclusive)
        }

        val event0 = ObjectData.create("""
            {
                "name": "event0",
                "att": "value",
                "str": "strValue",
                "num": 10
            }
        """)
        val event1 = ObjectData.create("""
            {
                "name": "event1",
                "att": "value",
                "str": "strValue2",
                "num": 20
            }
        """)
        val event2 = ObjectData.create("""
            {
                "name": "event2",
                "att": "value",
                "str": "strValue3",
                "num": 30
            }
        """)

        val eventsNumGE20List = ArrayList<String>()
        app1Events.addListener(baseListenerConfig.copy()
            .withFilter(Predicates.ge("num", 20.0))
            .withAction {
                eventsNumGE20List.add(it.get("name").asText())
            }
            .build())

        val eventsStrEqStrValue2OrStrValue3 = ArrayList<String>()
        app1Events.addListener(baseListenerConfig.copy()
            .withFilter(Predicates.or(
                Predicates.eq("str", "strValue2"),
                Predicates.eq("str", "strValue3")
            ))
            .withAction {
                eventsStrEqStrValue2OrStrValue3.add(it.get("name").asText())
            }
            .build())

        val emitterApp0 = app0Events.getEmitter<ObjectData> {
            withEventType("test-event")
            withEventClass(ObjectData::class.java)
        }
        val emitterApp1 = app1Events.getEmitter(emitterApp0.config.copy {})

        Thread.sleep(200)

        emitterApp0.emit(event0)
        emitterApp0.emit(event1)
        emitterApp0.emit(event2)

        Thread.sleep(200)

        assertThat(eventsNumGE20List).containsExactly("event1", "event2")
        assertThat(eventsStrEqStrValue2OrStrValue3).containsExactly("event1", "event2")

        eventsNumGE20List.clear()
        eventsStrEqStrValue2OrStrValue3.clear()

        emitterApp1.emit(event0)
        emitterApp1.emit(event1)
        emitterApp1.emit(event2)

        Thread.sleep(200)

        assertThat(eventsNumGE20List).containsExactly("event1", "event2")
        assertThat(eventsStrEqStrValue2OrStrValue3).containsExactly("event1", "event2")
    }
}