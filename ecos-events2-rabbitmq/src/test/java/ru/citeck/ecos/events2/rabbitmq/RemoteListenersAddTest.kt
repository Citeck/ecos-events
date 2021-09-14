package ru.citeck.ecos.events2.rabbitmq

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.rabbitmq.client.ConnectionFactory
import ecos.org.apache.curator.RetryPolicy
import ecos.org.apache.curator.framework.CuratorFramework
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.retry.RetryForever
import ecos.org.apache.curator.test.TestingServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.events2.EventService
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.zookeeper.EcosZooKeeper

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RemoteListenersAddTest {

    companion object {
        const val NODE_TYPE: String = "type"
    }

    private lateinit var servers: TestUtils.MockServers
    private lateinit var eventServiceEmitterApp0: EventsService

    private lateinit var eventServiceReceiverApp0: EventsService

    private val personIvanRecordRef = RecordRef.create(TestUtils.RECORD_SOURCE_TEMPLATE.format("app0"),
        "ivan").toString()
    private val personIvanRecord = PersonRecord("Ivan", "Petrov")

    @BeforeEach
    fun setUp() {

        servers = TestUtils.createServers()

        eventServiceEmitterApp0 = TestUtils.createApp("app0", servers, mapOf(
            Pair(personIvanRecordRef, personIvanRecord)
        ))

        eventServiceReceiverApp0 = TestUtils.createApp("app_rec_0", servers, emptyMap())
    }

    @Test
    fun addWithSameIdListenersTest() {

        var receiveData0: NodeData? = null
        var receiveData1: NodeData? = null
        val emitData0 = NodeData("13-ab-kk-0", "some data 0", personIvanRecord)

        eventServiceReceiverApp0.addListener(ListenerConfig.create<NodeData> {
            id = "config0"
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receiveData0 = evData
            }
        })

        eventServiceReceiverApp0.addListener(ListenerConfig.create<NodeData> {
            id = "config0"
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receiveData1 = evData
            }
        })

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(EmitterConfig.create {
            eventType = NODE_TYPE
            eventClass = NodeData::class.java
        })

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

        eventServiceReceiverApp0.addListener(ListenerConfig.create<NodeData> {
            id = "config0"
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receiveData0 = evData
            }
        })

        eventServiceReceiverApp0.addListener(ListenerConfig.create<NodeData> {
            id = "config0"
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receiveData1 = evData
            }
        })

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(EmitterConfig.create {
            eventType = NODE_TYPE
            eventClass = NodeData::class.java
        })

        emitter.emit(emitData0)
        Thread.sleep(500)
        assertEquals(emitData0, receiveData0)
        assertNull(receiveData1)

        eventServiceReceiverApp0.removeListener(ListenerConfig.create<NodeData> {
            id = "config0"
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receiveData0 = evData
            }
        })
        receiveData0 = null
        receiveData1 = null

        Thread.sleep(500)

        emitter.emit(emitData0)

        Thread.sleep(500)

        assertNull(receiveData0)
        assertNull(receiveData1)
    }

    @AfterEach
    fun tearDown() {
        servers.close()
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