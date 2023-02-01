package ru.citeck.ecos.events2.rabbitmq

import com.github.javafaker.Faker
import mu.KotlinLogging
import org.apache.commons.lang3.time.StopWatch
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestUtils
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import java.time.Duration
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RemoteEventsPerfomanceTest {

    companion object {
        const val NODE_TYPE: String = "type"
        val log = KotlinLogging.logger {}
    }

    private val faker = Faker()

    private lateinit var servers: TestUtils.MockServers

    private lateinit var eventServiceEmitterApp0: EventsService
    private lateinit var eventServiceEmitterApp1: EventsService
    private lateinit var eventServiceEmitterApp2: EventsService

    private lateinit var eventServiceReceiverApp0: EventsService
    private lateinit var eventServiceReceiverApp1: EventsService
    private lateinit var eventServiceReceiverApp2: EventsService

    private val personIvanRecordRef = RecordRef.create(
        TestUtils.RECORD_SOURCE_TEMPLATE.format("app0"),
        "ivan"
    ).toString()
    private val personIvanRecord = PersonRecord("Ivan", "Petrov")

    @BeforeEach
    fun setUp() {

        servers = TestUtils.createServers()

        eventServiceEmitterApp0 = TestUtils.createApp(
            "appEmit0", servers,
            mapOf(
                Pair(personIvanRecordRef, personIvanRecord)
            )
        )
        eventServiceEmitterApp1 = TestUtils.createApp(
            "appEmit1", servers,
            mapOf(
                Pair(personIvanRecordRef, personIvanRecord)
            )
        )
        eventServiceEmitterApp2 = TestUtils.createApp(
            "appEmit2", servers,
            mapOf(
                Pair(personIvanRecordRef, personIvanRecord)
            )
        )

        eventServiceReceiverApp0 = TestUtils.createApp("appRec0", servers, emptyMap())
        eventServiceReceiverApp1 = TestUtils.createApp("appRec1", servers, emptyMap())
        eventServiceReceiverApp2 = TestUtils.createApp("appRec2", servers, emptyMap())
    }

    @Test
    fun oneEmitterPerfomanceTest() {

        val dataCount = getDataCount(10_000)
        val maxTime = 10_000L

        val dataToEmit0 = generateRandomNodeData(dataCount)

        val receivedDataFromListener0 = mutableListOf<NodeData>()
        val receivedDataFromListener1 = mutableListOf<NodeData>()
        val receivedDataFromListener2 = mutableListOf<NodeData>()

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receivedDataFromListener0.add(evData)
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receivedDataFromListener1.add(evData)
                }
            }
        )

        eventServiceReceiverApp2.addListener(
            ListenerConfig.create {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receivedDataFromListener2.add(evData)
                }
            }
        )

        Thread.sleep(1000)

        val watch = StopWatch()
        watch.start()

        dataToEmit0.forEach {
            emitter.emit(it)
        }

        waitSize(receivedDataFromListener0, dataCount, "receivedDataFromListener0")
        waitSize(receivedDataFromListener1, dataCount, "receivedDataFromListener1")
        waitSize(receivedDataFromListener2, dataCount, "receivedDataFromListener2")

        watch.stop()
        val receiveTime = watch.time

        log.info {
            "oneEmitterPerfomanceTest receiveTime: $receiveTime"
        }

        assertThat(receiveTime).isLessThan(maxTime)

        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit0)
    }

    @Test
    fun twoEmitterPerfomanceTest() {

        val dataCount = getDataCount(10_000)
        val emitterCount = 2
        val maxTime = 15_000L

        val dataToEmit0 = generateRandomNodeData(dataCount)
        val dataToEmit1 = generateRandomNodeData(dataCount)

        val receivedDataFromListener0 = mutableListOf<NodeData>()
        val receivedDataFromListener1 = mutableListOf<NodeData>()
        val receivedDataFromListener2 = mutableListOf<NodeData>()

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receivedDataFromListener0.add(evData)
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receivedDataFromListener1.add(evData)
                }
            }
        )

        eventServiceReceiverApp2.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receivedDataFromListener2.add(evData)
                }
            }
        )

        Thread.sleep(1000)

        val watch = StopWatch()
        watch.start()

        dataToEmit0.forEach {
            emitter.emit(it)
        }

        dataToEmit1.forEach {
            emitter.emit(it)
        }

        val expCount = dataCount * emitterCount
        waitSize(receivedDataFromListener0, expCount, "receivedDataFromListener0")
        waitSize(receivedDataFromListener1, expCount, "receivedDataFromListener1")
        waitSize(receivedDataFromListener2, expCount, "receivedDataFromListener2")

        watch.stop()
        val receiveTime = watch.time

        log.info {
            "twoEmitterPerformanceTest receiveTime: $receiveTime"
        }

        assertThat(receiveTime).isLessThan(maxTime)

        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit1)

        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit1)

        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit1)
    }

    @Test
    fun threeEmitterPerfomanceTest() {

        val dataCount = getDataCount(10_000)
        val emitterCount = 3
        val maxTime = 17_000L

        val dataToEmit0 = generateRandomNodeData(dataCount)
        val dataToEmit1 = generateRandomNodeData(dataCount)
        val dataToEmit2 = generateRandomNodeData(dataCount)

        val receivedDataFromListener0 = mutableListOf<NodeData>()
        val receivedDataFromListener1 = mutableListOf<NodeData>()
        val receivedDataFromListener2 = mutableListOf<NodeData>()

        val emitter = eventServiceEmitterApp0.getEmitter<NodeData>(
            EmitterConfig.create {
                eventType = NODE_TYPE
                eventClass = NodeData::class.java
            }
        )

        eventServiceReceiverApp0.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                setAction { evData ->
                    receivedDataFromListener0.add(evData)
                }
            }
        )

        eventServiceReceiverApp1.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receivedDataFromListener1.add(evData)
                }
            }
        )

        eventServiceReceiverApp2.addListener(
            ListenerConfig.create<NodeData> {
                eventType = NODE_TYPE
                dataClass = NodeData::class.java
                withAction { evData ->
                    receivedDataFromListener2.add(evData)
                }
            }
        )

        Thread.sleep(1000)

        val watch = StopWatch()
        watch.start()

        dataToEmit0.forEach {
            emitter.emit(it)
        }

        dataToEmit1.forEach {
            emitter.emit(it)
        }

        dataToEmit2.forEach {
            emitter.emit(it)
        }

        val expCount = dataCount * emitterCount
        waitSize(receivedDataFromListener0, expCount, "receivedDataFromListener0")
        waitSize(receivedDataFromListener1, expCount, "receivedDataFromListener1")
        waitSize(receivedDataFromListener2, expCount, "receivedDataFromListener2")

        watch.stop()
        val receiveTime = watch.time

        log.info {
            "threeEmitterPerfomanceTest receiveTime: $receiveTime"
        }

        assertThat(receiveTime).isLessThan(maxTime)

        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit1)
        assertThat(receivedDataFromListener0).containsAnyElementsOf(dataToEmit2)

        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit1)
        assertThat(receivedDataFromListener1).containsAnyElementsOf(dataToEmit2)

        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit0)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit1)
        assertThat(receivedDataFromListener2).containsAnyElementsOf(dataToEmit2)
    }

    @AfterEach
    fun tearDown() {
        servers.close()
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

    private fun getDataCount(libCount: Int): Int {
        return if (System.getProperties().containsKey("ecos.env.webapp-tests")) {
            (libCount / 10).coerceAtLeast(1)
        } else {
            libCount
        }
    }

    private fun waitSize(collection: Collection<*>, expectedSize: Int, collectionName: String) {
        val timeout = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis()
        while (System.currentTimeMillis() < timeout && collection.size != expectedSize) {
            Thread.sleep(200)
        }
        if (collection.size != expectedSize) {
            fail<Any>("Collection '$collectionName' has incorrect size: ${collection.size} expected size: $expectedSize")
        }
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
