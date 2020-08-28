package ru.citeck.ecos.events

import org.junit.jupiter.api.Test
import ru.citeck.ecos.events.listener.ListenerConfig
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.RecordsServiceFactory
import ru.citeck.ecos.records2.graphql.meta.annotation.MetaAtt
import kotlin.test.assertEquals

class EventsTest {

    @Test
    fun test() {

        val records = RecordsServiceFactory()
        val factory = EventServiceFactory(records)

        val eventService = factory.eventService

        val data = ArrayList<DataClass>()

        eventService.addListener(ListenerConfig.create<DataClass> {
            eventType = "test-type"
            dataClass = DataClass::class.java
            setAction { evData, _ ->
                data.add(evData)
            }
        })

        val emitter = eventService.getEmitter<DataClass> {
            eventType = "test-type"
            eventClass = DataClass::class.java
        }

        val targetData = arrayListOf(
                DataClass("aa", "bb"),
                DataClass("cc", "dd"),
                DataClass("ee", "ff")
        )
        targetData.forEach { emitter.emit(RecordRef.EMPTY, it) }

        assertEquals(targetData, data)
    }

    data class DataClass(
        @MetaAtt("\$event.field0") var field0: String?,
        @MetaAtt("\$event.field1") var field1: String?
    )
}