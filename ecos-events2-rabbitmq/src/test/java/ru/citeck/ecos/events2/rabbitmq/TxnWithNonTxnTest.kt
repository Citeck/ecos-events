package ru.citeck.ecos.events2.rabbitmq

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.events2.rabbitmq.utils.TestAppsCtx
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import kotlin.test.Test

class TxnWithNonTxnTest {

    companion object {
        private const val EVENT_TYPE = "test"
    }

    private lateinit var eventsService0: EventsService
    private lateinit var eventsService1: EventsService

    @BeforeEach
    fun beforeEach() {
        val apps = TestAppsCtx()
        eventsService0 = apps.createApp("app0").eventsService
        eventsService1 = apps.createApp("app1").eventsService

        val txnManager = TransactionManagerImpl()
        txnManager.init(EcosWebAppApiMock("app1"))
        TxnContext.setManager(txnManager)
    }

    @Test
    fun test() {

        val txnEvents = ArrayList<TxnEventData>()

        eventsService0.addListener(
            ListenerConfig.create {
                withEventType(EVENT_TYPE)
                withTransactional(true)
                withDataClass(TxnEventData::class.java)
                withAction { txnEvents.add(it) }
            }
        )

        val nonTxnEvents = ArrayList<NonTxnEventData>()

        eventsService0.addListener(
            ListenerConfig.create {
                withEventType(EVENT_TYPE)
                withTransactional(false)
                withDataClass(NonTxnEventData::class.java)
                withAction { nonTxnEvents.add(it) }
            }
        )

        val app1Emitter = eventsService1.getEmitter {
            withSource("app1")
            withEventType(EVENT_TYPE)
            withEventClass(EventData::class.java)
        }

        TxnContext.doInNewTxn {
            app1Emitter.emit(EventData("aaa", "bbb"))
            assertThat(txnEvents).hasSize(1)
            assertThat(txnEvents.first().txnText).isEqualTo("aaa")
            Thread.sleep(1000)
            assertThat(nonTxnEvents).isEmpty()
        }
        Thread.sleep(1000)
        assertThat(nonTxnEvents).hasSize(1)
        assertThat(nonTxnEvents.first().nonTxnText).isEqualTo("bbb")

        txnEvents.clear()
        nonTxnEvents.clear()

        val app0Emitter = eventsService1.getEmitter {
            withSource("app0")
            withEventType(EVENT_TYPE)
            withEventClass(EventData::class.java)
        }

        TxnContext.doInNewTxn {
            app0Emitter.emit(EventData("ccc", "ddd"))
            assertThat(txnEvents).hasSize(1)
            assertThat(txnEvents.first().txnText).isEqualTo("ccc")
            Thread.sleep(1000)
            assertThat(nonTxnEvents).isEmpty()
        }
        Thread.sleep(1000)
        assertThat(nonTxnEvents).hasSize(1)
        assertThat(nonTxnEvents.first().nonTxnText).isEqualTo("ddd")
    }

    class EventData(
        val txnText: String,
        val nonTxnText: String
    )

    class TxnEventData(
        val txnText: String
    )

    class NonTxnEventData(
        val nonTxnText: String
    )
}
