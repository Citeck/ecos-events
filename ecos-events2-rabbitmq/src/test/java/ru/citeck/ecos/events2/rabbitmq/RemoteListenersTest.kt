package ru.citeck.ecos.events2.rabbitmq

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestAppsCtx
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RemoteListenersTest {

    companion object {
        const val NODE_TYPE: String = "type"
    }

    private lateinit var servers: TestAppsCtx
    private lateinit var eventServiceEmitterApp0: EventsService
    private lateinit var eventServiceReceiverApp1: EventsService

    private val personIvanLocalId = "ivan"
    private val personIvanRecord = PersonRecord(
        "Ivan",
        "Petrov",
        listOf(
            "89002003050",
            "89001001003987"
        )
    )

    @BeforeEach
    fun setUp() {
        servers = TestAppsCtx()

        val eventServiceEmitterApp0Ctx = servers.createApp("app0")
        eventServiceEmitterApp0Ctx.registerRecord(personIvanLocalId, personIvanRecord)
        eventServiceEmitterApp0 = eventServiceEmitterApp0Ctx.eventsService
        eventServiceReceiverApp1 = servers.createApp("app1").eventsService
    }

    @Test
    fun firstEmitterRegistrationTest() {

        var receiveData: NodeData? = null
        val emitData = NodeData("13-ab-kk", "data")

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                setAction { evData ->
                    receiveData = evData
                }
            }
        )

        Thread.sleep(1000)

        emitter.emit(emitData)

        Thread.sleep(1000)

        assertEquals(emitData, receiveData)
    }

    @Test
    fun firstReceiverRegistrationTest() {

        var receiveData: NodeData? = null
        val emitData = NodeData("13-ab-kk-1", "some data")

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                setAction { evData ->
                    receiveData = evData
                }
            }
        )

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData)

        Thread.sleep(1000)

        assertEquals(emitData, receiveData)
    }

    @Test
    fun receiveDataWithRecordMetaTest() {

        var receiveData: NodeDataWithCreatorMeta? = null
        val emitData = NodeData("13-ab-kk-1", "some data", personIvanRecord)

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeDataWithCreatorMeta> {
                eventType = NODE_TYPE
                dataClass = NodeDataWithCreatorMeta::class.java
                withAction { evData ->
                    receiveData = evData
                }
            }
        )

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        emitter.emit(emitData)

        Thread.sleep(1000)

        assertEquals(emitData.id, receiveData!!.id)
        assertEquals(emitData.data, receiveData!!.data)
        assertEquals(emitData.creator, receiveData!!.creator)
        assertEquals(personIvanRecord.firstName, receiveData!!.creatorFirstName)
        assertEquals(personIvanRecord.lastName, receiveData!!.creatorLastName)
        Assertions.assertThat(emitData.creator!!.phones).containsExactlyInAnyOrderElementsOf(receiveData!!.creatorPhones)
    }

    @Test
    fun updateListenerDataTest() {

        var receiveData0: NodeData? = null
        var receiveData1: NodeDataWithCreatorMeta? = null

        val emitData0 = NodeData("13-ab-kk-0", "some data 0", personIvanRecord)
        val emitData1 = NodeData("13-ab-kk-1", "some data 1", personIvanRecord)

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                setAction { evData ->
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
        Thread.sleep(1000)
        assertEquals(emitData0, receiveData0)

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeDataWithCreatorMeta> {
                eventType = NODE_TYPE
                dataClass = NodeDataWithCreatorMeta::class.java
                withAction { evData ->
                    receiveData1 = evData
                }
            }
        )

        Thread.sleep(500)
        val listenerWithCreatorMeta = servers.zookeeper.getValue(
            "/events/$NODE_TYPE/app1",
            ZkAppEventListener::class.java
        )
        assertEquals(6, listenerWithCreatorMeta!!.attributes.size)

        emitter.emit(emitData1)
        Thread.sleep(1000)

        assertEquals(emitData1.id, receiveData1!!.id)
        assertEquals(emitData1.data, receiveData1!!.data)
        assertEquals(emitData1.creator, receiveData1!!.creator)
        assertEquals(personIvanRecord.firstName, receiveData1!!.creatorFirstName)
        assertEquals(personIvanRecord.lastName, receiveData1!!.creatorLastName)
    }

    @Test
    fun multipleListenersInOneAppWithDifferentDataSchemaTest() {

        var receiveDataMinimal: MinimalNodeData? = null
        var receiveDataMedium: MediumNodeData? = null
        var receiveDataOne: OneNodeData? = null

        val emitData = FullNodeData("13-ab-kk-0", "some data 0", 2, "Galina", Date())

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<MinimalNodeData> {
                eventType = NODE_TYPE
                dataClass = MinimalNodeData::class.java
                setAction { evData ->
                    receiveDataMinimal = evData
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<MediumNodeData> {
                eventType = NODE_TYPE
                dataClass = MediumNodeData::class.java
                setAction { evData ->
                    receiveDataMedium = evData
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<OneNodeData> {
                eventType = NODE_TYPE
                dataClass = OneNodeData::class.java
                setAction { evData ->
                    receiveDataOne = evData
                }
            }
        )

        val emitter = eventServiceEmitterApp0.getEmitter<FullNodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = FullNodeData::class.java
            }
        )

        Thread.sleep(1000)

        val listenerWithCreatorMeta = servers.zookeeper.getValue(
            "/events/$NODE_TYPE/app1",
            ZkAppEventListener::class.java
        )

        assertEquals(5, listenerWithCreatorMeta!!.attributes.size)

        emitter.emit(emitData)

        Thread.sleep(1000)

        assertEquals(emitData.id, receiveDataMinimal!!.id)
        assertEquals(emitData.data, receiveDataMinimal!!.data)

        assertEquals(emitData.id, receiveDataMedium!!.id)
        assertEquals(emitData.data, receiveDataMedium!!.data)
        assertEquals(emitData.modified, receiveDataMedium!!.modified)
        assertEquals(emitData.version, receiveDataMedium!!.version)

        assertEquals(emitData.creator, receiveDataOne!!.creator)
    }

    private data class NodeData(
        val id: String,
        val data: String,
        val creator: PersonRecord? = null
    )

    private data class NodeDataWithCreatorMeta(
        val id: String,
        val data: String,
        val creator: PersonRecord? = null,

        @AttName("creator.firstName")
        val creatorFirstName: String? = null,

        @AttName("creator.lastName")
        val creatorLastName: String? = null,

        @AttName("creator.phones[]")
        val creatorPhones: List<String>
    )

    private data class PersonRecord(
        val firstName: String,
        val lastName: String,
        val phones: List<String>
    )

    private data class FullNodeData(
        val id: String,
        val data: String,
        val version: Int,
        val creator: String,
        val modified: Date
    )

    private data class MinimalNodeData(
        val id: String,
        val data: String
    )

    private data class MediumNodeData(
        val id: String,
        val data: String,
        val modified: Date,
        val version: Int
    )

    private data class OneNodeData(
        val creator: String
    )
}
