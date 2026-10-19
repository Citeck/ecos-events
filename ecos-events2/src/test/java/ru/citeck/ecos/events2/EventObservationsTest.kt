package ru.citeck.ecos.events2

import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.obs.EventsObsCtx
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.micrometer.obs.EcosObs
import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class EventObservationsTest {

    private lateinit var appMock: EcosWebAppApiMock
    private lateinit var txnManager: TransactionManagerImpl

    @BeforeEach
    fun setUpTransactionManager() {

        txnManager = TransactionManagerImpl()
        appMock = EcosWebAppApiMock()
        txnManager.init(appMock, EcosTxnProps())
        TxnContext.setManager(txnManager)
    }

    @AfterEach
    fun shutDownTransactionManager() {

        txnManager.shutdown()
        appMock.dispose()
    }

    private val registry = ObservationRegistry.create()
    private val stopped = CopyOnWriteArrayList<Observation.Context>()
    private val actionStopped = CountDownLatch(1)
    private val meter = object : EcosMicrometerContext {
        override fun createObs(name: String): EcosObs = createObs(EcosObsContext(name))
        override fun createObs(context: EcosObsContext): EcosObs {

            return EcosObs(Observation.createNotStarted(context.name, { context }, registry))
        }
    }
    private val service = EventsServiceFactory().apply {
        recordsServices = object : RecordsServiceFactory() {
            override fun createMicrometerContext(): EcosMicrometerContext = meter
        }
    }.eventsService

    init {
        registry.observationConfig().observationHandler(object : ObservationHandler<Observation.Context> {
            override fun supportsContext(context: Observation.Context): Boolean = true
            override fun onStop(context: Observation.Context) {

                stopped.add(context)
                if (context.name == "ecos.events.listener.action") actionStopped.countDown()
            }
        })
    }

    @Test
    fun listenerActionsAreNestedAndIdentifiable() {

        val calls = mutableListOf<String>()
        for (id in listOf("first", "second")) {
            service.addListener(
                ListenerConfig.create<Unit> {
                    withId(id)
                    withEventType("test")
                    withDataClass(Unit::class.java)
                    withTransactional(true)
                    withAction {
                        calls.add(id)
                        meter.createObs("downstream").observe {}
                    }
                }
            )
        }
        val event = event()
        service.emitEventFromRemote(event, true, true)

        assertEquals(listOf("first", "second"), calls)
        val actions = stopped.filter { it.name == "ecos.events.listener.action" }
        assertEquals(listOf("first", "second"), actions.map { (it as EventsObsCtx.Listener).listener.config.id })
        actions.forEach {
            val actionContext = it as EventsObsCtx.Listener
            assertSame(event, actionContext.event)
            stopped.filterIsInstance<EventsObsCtx.Listener>()
                .filter { context -> context.listener.config.id == actionContext.listener.config.id }
                .forEach { context -> assertSame(actionContext.listener, context.listener) }
            assertEquals("ecos.events.listener", it.parentObservation!!.contextView.name)
        }
        stopped.filter { it.name == "downstream" }.forEach {
            assertEquals("ecos.events.listener.action", it.parentObservation!!.contextView.name)
        }
        assertNull(registry.currentObservation)
        assertTrue(stopped.all { it.lowCardinalityKeyValues.none { kv -> kv.key == "event.id" } })
    }

    @Test
    fun failureIsRecordedAndOriginalExceptionPropagates() {

        val failure = IllegalStateException("test failure")
        service.addListener(
            ListenerConfig.create<Unit> {
                withId("failing")
                withEventType("test")
                withDataClass(Unit::class.java)
                withTransactional(true)
                withAction { throw failure }
            }
        )
        assertSame(
            failure,
            assertThrows(IllegalStateException::class.java) {
                service.emitEventFromRemote(event(), true, true)
            }
        )
        assertSame(failure, stopped.single { it.name == "ecos.events.listener.action" }.error)
        assertSame(failure, stopped.single { it.name == "ecos.events.remote.dispatch" }.error)
        assertNull(registry.currentObservation)
    }

    @Test
    fun rejectedFilterDoesNotInvokeActionOrConvertPayload() {

        service.addListener(
            ListenerConfig.create<Unit> {
                withId("filtered")
                withEventType("test")
                withDataClass(Unit::class.java)
                withTransactional(true)
                withFilter(Predicates.eq("enabled", true))
                withAction { fail<Unit>("Filtered listener must not run") }
            }
        )
        service.emitEventFromRemote(event(), true, true)
        assertTrue(stopped.any { it.name == "ecos.events.listener.filter" })
        assertFalse(stopped.any { it.name == "ecos.events.listener.action" || it.name == "ecos.events.listener.convert" })
    }

    @Test
    fun deferredActionIsObservedAtExecutionTime() {

        val called = AtomicBoolean(false)
        service.addListener(
            ListenerConfig.create<Unit> {
                withId("after-commit")
                withEventType("test")
                withDataClass(Unit::class.java)
                withAction {
                    called.set(true)
                    assertEquals("ecos.events.listener.action", registry.currentObservation!!.contextView.name)
                }
            }
        )
        TxnContext.doInNewTxn {
            service.getEmitter<Unit> {
                withEventType("test")
                withEventClass(Unit::class.java)
            }.emit(Unit)
            assertFalse(called.get())
            assertFalse(stopped.any { it.name == "ecos.events.listener.action" })
        }
        assertTrue(actionStopped.await(5, TimeUnit.SECONDS), "Deferred listener did not finish")
        assertTrue(called.get())
        assertEquals(1, stopped.count { it.name == "ecos.events.listener.action" })
        assertNull(registry.currentObservation)
    }

    private fun event(): EcosEvent = EcosEvent(
        UUID.randomUUID(),
        Instant.now(),
        "test",
        "test-user",
        EventsSource("test", "test", "test"),
        ObjectData.create()
    )
}
