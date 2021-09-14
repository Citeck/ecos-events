package ru.citeck.ecos.events2

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.listener.ListenerConfig
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import java.time.Instant

class EventsTest {

    @Test
    fun receiveEventTest() {

        val records = RecordsServiceFactory()
        val factory = EventServiceFactory()
        factory.recordsServices = records

        val eventService = factory.eventService

        val data = ArrayList<DataClass>()

        eventService.addListener(ListenerConfig.create<DataClass> {
            eventType = "test-type"
            dataClass = DataClass::class.java
            setAction { evData ->
                data.add(evData)
            }
        })

        val emitter = eventService.getEmitter<DataClass>(EmitterConfig.create {
            eventType = "test-type"
            eventClass = DataClass::class.java
        })

        val targetData = arrayListOf(
            DataClass("aa", "bb"),
            DataClass("cc", "dd"),
            DataClass("ee", "ff")
        )
        targetData.forEach { emitter.emit(it) }

        assertEquals(targetData, data)
    }

    @Test
    fun receiveEventWithEventInfoTest() {

        val records = RecordsServiceFactory()
        val factory = EventServiceFactory()
        factory.recordsServices = records

        val eventService = factory.eventService

        val emitData = DataClass("aa", "bb")
        var receiveData: DataClassWithEventInfo? = null

        val userIvan = "ivan.petrov"

        eventService.addListener(ListenerConfig.create<DataClassWithEventInfo> {
            eventType = "test-type"
            dataClass = DataClassWithEventInfo::class.java
            setAction { evData ->
                receiveData = evData
            }
        })

        val emitter = eventService.getEmitter<DataClass>(EmitterConfig.create {
            eventType = "test-type"
            eventClass = DataClass::class.java
        })

        AuthContext.runAs(userIvan) {
            emitter.emit(emitData)
        }

        assertNotNull(receiveData)
        assertEquals(emitData.field0, receiveData!!.field0)
        assertEquals(emitData.field1, receiveData!!.field1)

        assertTrue(receiveData!!.eventId.isNotBlank())
        assertNotNull(receiveData!!.eventTime)
        assertEquals(userIvan, receiveData!!.eventUser)
    }

    private data class DataClass(
        @AttName("field0") val field0: String,
        @AttName("field1") val field1: String
    )

    private data class DataClassWithEventInfo(
        val field0: String,
        val field1: String,

        @AttName("\$event.id")
        val eventId: String,

        @AttName("\$event.time")
        val eventTime: Instant,

        @AttName("\$event.user")
        val eventUser: String
    )
}