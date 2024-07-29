package ru.citeck.ecos.events2.rabbitmq

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RemoteListenersRemovalTest {

    companion object {
        const val NODE_TYPE: String = "type"
    }

    private lateinit var servers: TestUtils.MockServers

    private lateinit var eventServiceEmitterApp0: EventsService
    private lateinit var eventServiceEmitterApp1: EventsService

    private lateinit var eventServiceReceiverApp0: EventsService
    private lateinit var eventServiceReceiverApp1: EventsService
    private lateinit var eventServiceReceiverApp2: EventsService

    private val personIvanRecordRef = EntityRef.create(
        TestUtils.RECORD_SOURCE_TEMPLATE.format("app0"),
        "ivan"
    ).toString()
    private val personIvanRecord = PersonRecord("Ivan", "Petrov")

    @BeforeEach
    fun setUp() {
        servers = TestUtils.createServers()

        eventServiceEmitterApp0 = TestUtils.createApp(
            "app0",
            servers,
            mapOf(
                Pair(personIvanRecordRef, personIvanRecord)
            )
        )
        eventServiceEmitterApp1 = TestUtils.createApp(
            "app1",
            servers,
            mapOf(
                Pair(personIvanRecordRef, personIvanRecord)
            )
        )

        eventServiceReceiverApp0 = TestUtils.createApp("app_rec_0", servers, emptyMap())
        eventServiceReceiverApp1 = TestUtils.createApp("app_rec_1", servers, emptyMap())
        eventServiceReceiverApp2 = TestUtils.createApp("app_rec_2", servers, emptyMap())
    }

    @Test
    fun removeListenersTest() {

        var receiveData0: NodeData? = null
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

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData0)
        Thread.sleep(500)
        assertEquals(emitData0, receiveData0)

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

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertNull(receiveData0)
    }

    @Test
    fun removeListenersByIdTest() {

        var receiveData0: NodeData? = null
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

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData0)
        Thread.sleep(500)
        assertEquals(emitData0, receiveData0)

        eventServiceReceiverApp0.removeListener("config0")
        receiveData0 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertNull(receiveData0)
    }

    @Test
    fun multipleRemoveListenersTest() {

        var receiveData0: NodeData? = null
        var receiveData1: NodeData? = null
        var receiveData2: NodeData? = null

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
                id = "config1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData1 = evData
                }
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config2"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData2 = evData
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
        assertEquals(emitData0, receiveData1)
        assertEquals(emitData0, receiveData2)

        eventServiceReceiverApp0.removeListener("config0")
        receiveData0 = null
        receiveData1 = null
        receiveData2 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertNull(receiveData0)
        assertEquals(emitData0, receiveData1)
        assertEquals(emitData0, receiveData2)
    }

    @Test
    fun multipleRemoveListenersByHandleTest() {

        var receiveData0: NodeData? = null
        var receiveData1: NodeData? = null
        var receiveData2: NodeData? = null

        val emitData0 = NodeData("13-ab-kk-0", "some data 0", personIvanRecord)

        val listenerHandle0 = eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                setAction { evData ->
                    receiveData0 = evData
                }
            }
        )

        val listenerHandle1 = eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                setAction { evData ->
                    receiveData1 = evData
                }
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config2"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData2 = evData
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
        assertEquals(emitData0, receiveData1)
        assertEquals(emitData0, receiveData2)

        listenerHandle0.remove()
        listenerHandle1.remove()
        receiveData0 = null
        receiveData1 = null
        receiveData2 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertNull(receiveData0)
        assertNull(receiveData1)
        assertEquals(emitData0, receiveData2)
    }

    @Test
    fun multipleRemoveListenersMultipleEmittersTest() {

        val receiveData00: MutableList<NodeData> = mutableListOf()
        val receiveData01: MutableList<NodeData> = mutableListOf()
        val receiveData10: MutableList<NodeData> = mutableListOf()
        val receiveData11: MutableList<NodeData> = mutableListOf()

        val emitData0 = NodeData("13-ab-kk-0", "some data 0", personIvanRecord)
        val emitData1 = NodeData("13-ab-kk-1", "some data 1", personIvanRecord)

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_0_0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData00.add(evData)
                }
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_0_1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData01.add(evData)
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_1_0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData10.add(evData)
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_1_1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData11.add(evData)
                }
            }
        )

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        val emitter1 = eventServiceEmitterApp1.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData0)
        emitter1.emit(emitData1)

        Thread.sleep(500)
        Assertions.assertThat(receiveData00).containsAnyElementsOf(listOf(emitData0, emitData1))
        Assertions.assertThat(receiveData01).containsAnyElementsOf(listOf(emitData0, emitData1))
        Assertions.assertThat(receiveData10).containsAnyElementsOf(listOf(emitData0, emitData1))
        Assertions.assertThat(receiveData11).containsAnyElementsOf(listOf(emitData0, emitData1))

        eventServiceReceiverApp1.removeListener("config_1_1")
        eventServiceReceiverApp1.removeListener("config_0_0")
        receiveData00.clear()
        receiveData01.clear()
        receiveData10.clear()
        receiveData11.clear()

        Thread.sleep(500)

        emitter.emit(emitData0)
        emitter1.emit(emitData1)

        Thread.sleep(500)

        Assertions.assertThat(receiveData00).containsAnyElementsOf(listOf(emitData0, emitData1))
        Assertions.assertThat(receiveData01).containsAnyElementsOf(listOf(emitData0, emitData1))
        Assertions.assertThat(receiveData10).containsAnyElementsOf(listOf(emitData0, emitData1))
        Assertions.assertThat(receiveData11).isEmpty()
    }

    @Test
    fun multipleAppRemoveListenersTest() {

        var receiveData00: NodeData? = null
        var receiveData01: NodeData? = null
        var receiveData10: NodeData? = null
        var receiveData11: NodeData? = null

        val emitData0 = NodeData("13-ab-kk-0", "some data 0", personIvanRecord)

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_0_0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData00 = evData
                }
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_0_1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData01 = evData
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_1_0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData10 = evData
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_1_1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData11 = evData
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
        assertEquals(emitData0, receiveData00)
        assertEquals(emitData0, receiveData01)
        assertEquals(emitData0, receiveData10)
        assertEquals(emitData0, receiveData11)

        eventServiceReceiverApp0.removeListener("config_0_1")
        receiveData00 = null
        receiveData01 = null
        receiveData10 = null
        receiveData11 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertEquals(emitData0, receiveData00)
        assertEquals(null, receiveData01)
        assertEquals(emitData0, receiveData10)
        assertEquals(emitData0, receiveData11)
    }

    @Test
    fun multipleAppRemoveListenersWithSameIdTest() {

        var receiveData00: NodeData? = null
        var receiveData01: NodeData? = null
        var receiveData10: NodeData? = null
        var receiveData11: NodeData? = null

        val emitData0 = NodeData("13-ab-kk-0", "some data 0", personIvanRecord)

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_0_0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData00 = evData
                }
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_0_1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData01 = evData
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_1_0"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData10 = evData
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                id = "config_1_1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData11 = evData
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
        assertEquals(emitData0, receiveData00)
        assertEquals(emitData0, receiveData01)
        assertEquals(emitData0, receiveData10)
        assertEquals(emitData0, receiveData11)

        eventServiceReceiverApp0.removeListener("config_0_1")
        eventServiceReceiverApp0.removeListener("config_1_1")
        eventServiceReceiverApp0.removeListener("config_1_0")
        receiveData00 = null
        receiveData01 = null
        receiveData10 = null
        receiveData11 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertEquals(emitData0, receiveData00)
        assertEquals(null, receiveData01)
        assertEquals(emitData0, receiveData10)
        assertEquals(emitData0, receiveData11)
    }

    @Test
    fun removeNonExistentListenersTest() {

        var receiveData0: NodeData? = null
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

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData0)
        Thread.sleep(500)
        assertEquals(emitData0, receiveData0)

        eventServiceReceiverApp0.removeListener(
            ListenerConfig.create<NodeData> {
                id = "config1"
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receiveData0 = evData
                }
            }
        )
        receiveData0 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertEquals(emitData0, receiveData0)
    }

    @Test
    fun removeNonExistentByIdListenersTest() {

        var receiveData0: NodeData? = null
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

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData0)
        Thread.sleep(500)
        assertEquals(emitData0, receiveData0)

        eventServiceReceiverApp0.removeListener("config1")
        receiveData0 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertEquals(emitData0, receiveData0)
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
