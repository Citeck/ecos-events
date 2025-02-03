package ru.citeck.ecos.events2.rabbitmq

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestAppsCtx

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RemoteListenersAddTest {

    companion object {
        const val NODE_TYPE: String = "type"
    }

    private lateinit var servers: TestAppsCtx
    private lateinit var eventServiceEmitterApp0: EventsService

    private lateinit var eventServiceReceiverApp0: EventsService

    private val personIvanLocalId = "ivan"

    private val personIvanRecord = PersonRecord("Ivan", "Petrov")

    @BeforeEach
    fun setUp() {

        servers = TestAppsCtx()

        val app0Ctx = servers.createApp("app0")
        app0Ctx.registerRecord(personIvanLocalId, personIvanRecord)
        eventServiceEmitterApp0 = app0Ctx.eventsService

        eventServiceReceiverApp0 = servers.createApp("app_rec_0").eventsService
    }

    @Test
    fun addWithSameIdListenersTest() {

        var receiveData0: NodeData? = null
        var receiveData1: NodeData? = null
        val emitData0 = NodeData("13-ab-kk-0", "some data 0", personIvanRecord)

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData0 = evData
                }
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData1 = evData
                }
            }
        )

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertEquals(emitData0, receiveData0)
        assertNull(receiveData1)
    }

    @Test
    fun addWithSameIdListenersRemovalTest() {

        var receiveData0: NodeData? = null
        var receiveData1: NodeData? = null
        val emitData0 = NodeData("13-ab-kk-0", "some data 0", personIvanRecord)

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData0 = evData
                }
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData1 = evData
                }
            }
        )

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData0)
        Thread.sleep(500)
        assertEquals(emitData0, receiveData0)
        assertNull(receiveData1)

        eventServiceReceiverApp0.removeListener(
            ListenerConfig.create<NodeData> {
                id = "config0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData0 = evData
                }
            }
        )
        receiveData0 = null
        receiveData1 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertNull(receiveData0)
        assertNull(receiveData1)
    }

    private data class NodeData(
        val id: String,
        val data: String,
        val creator: PersonRecord? = null
    )

    private data class PersonRecord(
        val firstName: String,
        val lastName: String
    )
}
