package ru.citeck.ecos.events2.rabbitmq

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.github.javafaker.Faker
import com.rabbitmq.client.ConnectionFactory
import ecos.org.apache.curator.RetryPolicy
import ecos.org.apache.curator.framework.CuratorFramework
import ecos.org.apache.curator.framework.CuratorFrameworkFactory
import ecos.org.apache.curator.retry.RetryForever
import ecos.org.apache.curator.test.TestingServer
import mu.KotlinLogging
import org.apache.commons.lang3.time.StopWatch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.events2.EventService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RemoteEventsPerfomanceTest {

    companion object {
        const val NODE_TYPE: String = "type"
        val log = KotlinLogging.logger {}
    }

    private val faker = Faker()

    private var zkServer: TestingServer? = null

    private lateinit var zkClient: CuratorFramework
    private lateinit var ecosZooKeeper: EcosZooKeeper

    private lateinit var eventServiceEmitterApp0: EventService
    private lateinit var eventServiceEmitterApp1: EventService
    private lateinit var eventServiceEmitterApp2: EventService

    private lateinit var eventServiceReceiverApp0: EventService
    private lateinit var eventServiceReceiverApp1: EventService
    private lateinit var eventServiceReceiverApp2: EventService

    private val personIvanRecordRef = RecordRef.create(
        TestUtils.RECORD_SOURCE_TEMPLATE.format("app0"),
        "ivan"
    ).toString()
    private val personIvanRecord = PersonRecord("Ivan", "Petrov")

    @BeforeEach
    fun setUp() {
        zkServer = TestingServer()

        val retryPolicy: RetryPolicy = RetryForever(7_000)

        zkClient = CuratorFrameworkFactory
            .newClient(zkServer!!.connectString, retryPolicy)
        zkClient.start()
        ecosZooKeeper = EcosZooKeeper(zkClient).withNamespace("ecos")

        val factory: ConnectionFactory = MockConnectionFactory()
        val connection = RabbitMqConn(factory)

        connection.waitUntilReady(5_000)

        eventServiceEmitterApp0 = TestUtils.createApp(
            "appEmit0", connection, ecosZooKeeper, mapOf(
                Pair(personIvanRecordRef, personIvanRecord)
            )
        )
        eventServiceEmitterApp1 = TestUtils.createApp(
            "appEmit1", connection, ecosZooKeeper, mapOf(
                Pair(personIvanRecordRef, personIvanRecord)
            )
        )
        eventServiceEmitterApp2 = TestUtils.createApp(
            "appEmit2", connection, ecosZooKeeper, mapOf(
                Pair(personIvanRecordRef, personIvanRecord)
            )
        )

        eventServiceReceiverApp0 = TestUtils.createApp("appRec0", connection, ecosZooKeeper, emptyMap())
        eventServiceReceiverApp1 = TestUtils.createApp("appRec1", connection, ecosZooKeeper, emptyMap())
        eventServiceReceiverApp2 = TestUtils.createApp("appRec2", connection, ecosZooKeeper, emptyMap())
    }

    @Test
    fun oneEmitterPerfomanceTest() {

        val dataCount = 10_000
        val maxTime = 7_000L

        val dataToEmit0 = generateRandomNodeData(dataCount)

        val watch = StopWatch()
        watch.start()

        val receivedDataFromListener0 = mutableListOf<NodeData>()
        val receivedDataFromListener1 = mutableListOf<NodeData>()
        val receivedDataFromListener2 = mutableListOf<NodeData>()

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(EmitterConfig.create {
            eventType = NODE_TYPE
            eventClass = NodeData::class.java
        })

        eventServiceReceiverApp0.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener0.add(evData)
            }
        })

        eventServiceReceiverApp1.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener1.add(evData)
            }
        })

        eventServiceReceiverApp2.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener2.add(evData)
            }
        })

        Thread.sleep(1000)

        dataToEmit0.forEach {
            emitter.emit(it)
        }

        Thread.sleep(1000)

        watch.stop()
        val receiveTime = watch.time

        log.info {
            "oneEmitterPerfomanceTest receiveTime: $receiveTime"
        }

        assertThat(receiveTime).isLessThan(maxTime)

        assertThat(receivedDataFromListener0.size).isEqualTo(dataCount)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit0)

        assertThat(receivedDataFromListener1.size).isEqualTo(dataCount)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit0)

        assertThat(receivedDataFromListener2.size).isEqualTo(dataCount)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit0)
    }

    @Test
    fun twoEmitterPerfomanceTest() {

        val dataCount = 10_000
        val emitterCount = 2
        val maxTime = 10_000L

        val dataToEmit0 = generateRandomNodeData(dataCount)
        val dataToEmit1 = generateRandomNodeData(dataCount)

        val watch = StopWatch()
        watch.start()

        val receivedDataFromListener0 = mutableListOf<NodeData>()
        val receivedDataFromListener1 = mutableListOf<NodeData>()
        val receivedDataFromListener2 = mutableListOf<NodeData>()

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(EmitterConfig.create {
            eventType = NODE_TYPE
            eventClass = NodeData::class.java
        })

        eventServiceReceiverApp0.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener0.add(evData)
            }
        })

        eventServiceReceiverApp1.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener1.add(evData)
            }
        })

        eventServiceReceiverApp2.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener2.add(evData)
            }
        })

        Thread.sleep(1000)

        dataToEmit0.forEach {
            emitter.emit(it)
        }

        dataToEmit1.forEach {
            emitter.emit(it)
        }

        Thread.sleep(1000)

        watch.stop()
        val receiveTime = watch.time

        log.info {
            "twoEmitterPerfomanceTest receiveTime: $receiveTime"
        }

        assertThat(receiveTime).isLessThan(maxTime)

        assertThat(receivedDataFromListener0.size).isEqualTo(dataCount * emitterCount)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit1)

        assertThat(receivedDataFromListener1.size).isEqualTo(dataCount * emitterCount)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit1)

        assertThat(receivedDataFromListener2.size).isEqualTo(dataCount * emitterCount)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit1)
    }

    @Test
    fun threeEmitterPerfomanceTest() {

        val dataCount = 10_000
        val emitterCount = 3
        val maxTime = 13_000L

        val dataToEmit0 = generateRandomNodeData(dataCount)
        val dataToEmit1 = generateRandomNodeData(dataCount)
        val dataToEmit2 = generateRandomNodeData(dataCount)

        val watch = StopWatch()
        watch.start()

        val receivedDataFromListener0 = mutableListOf<NodeData>()
        val receivedDataFromListener1 = mutableListOf<NodeData>()
        val receivedDataFromListener2 = mutableListOf<NodeData>()

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(EmitterConfig.create {
            eventType = NODE_TYPE
            eventClass = NodeData::class.java
        })

        eventServiceReceiverApp0.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener0.add(evData)
            }
        })

        eventServiceReceiverApp1.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener1.add(evData)
            }
        })

        eventServiceReceiverApp2.addListener(ListenerConfig.create<NodeData> {
            eventType = NODE_TYPE
            dataClass = NodeData::class.java
            setAction { evData ->
                receivedDataFromListener2.add(evData)
            }
        })

        Thread.sleep(1000)

        dataToEmit0.forEach {
            emitter.emit(it)
        }

        dataToEmit1.forEach {
            emitter.emit(it)
        }

        dataToEmit2.forEach {
            emitter.emit(it)
        }

        Thread.sleep(1000)

        watch.stop()
        val receiveTime = watch.time

        log.info {
            "threeEmitterPerfomanceTest receiveTime: $receiveTime"
        }

        assertThat(receiveTime).isLessThan(maxTime)

        assertThat(receivedDataFromListener0.size).isEqualTo(dataCount * emitterCount)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit1)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit2)

        assertThat(receivedDataFromListener1.size).isEqualTo(dataCount * emitterCount)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit1)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit2)

        assertThat(receivedDataFromListener2.size).isEqualTo(dataCount * emitterCount)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit1)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit2)

    }


    @AfterEach
    fun tearDown() {
        zkServer?.stop()
    }

    private fun generateRandomNodeData(size: Int): List<NodeData> {

        val data = mutableListOf<NodeData>()

        repeat(size) {
            val person = PersonRecord(faker.name().firstName(), faker.name().lastName())

            val nodeData = NodeData(
                UUID.randomUUID().toString(),
                faker.howIMetYourMother().catchPhrase(),
                creator = person,
                version = faker.number().randomDigit(),
                creatorFirstName = person.firstName,
                creatorLastName = person.lastName
            )

            data.add(nodeData)
        }

        return data
    }

    private data class NodeData(
        val id: String,
        val data: String,
        val creator: PersonRecord? = null,
        val version: Int,

        @AttName("creator.firstName")
        val creatorFirstName: String? = null,

        @AttName("creator.lastName")
        val creatorLastName: String? = null
    )

    private data class PersonRecord(
        val firstName: String,
        val lastName: String
    )

}