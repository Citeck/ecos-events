package ru.citeck.ecos.events2

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.events2.type.RecordChangedEvent
import ru.citeck.ecos.events2.type.RecordEventsService
import ru.citeck.ecos.events2.type.RecordStatusChangedEvent
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class RecEventsTest {

    companion object {
        private const val TEST_TYPE_ID = "test-type"
    }

    private val typesInfo = HashMap<String, TypeInfo>()
    private lateinit var services: EventsServiceFactory
    private lateinit var recEventsService: RecordEventsService

    @BeforeEach
    fun beforeEach() {

        typesInfo.clear()

        services = EventsServiceFactory()
        services.recordsServices = RecordsServiceFactory()
        services.modelServices = object : ModelServiceFactory() {
            override fun createTypesRepo(): TypesRepo {
                return object : TypesRepo {
                    override fun getChildren(typeRef: EntityRef): List<EntityRef> {
                        return emptyList()
                    }
                    override fun getTypeInfo(typeRef: EntityRef): TypeInfo? {
                        return typesInfo[typeRef.getLocalId()]
                    }
                }
            }
        }
        recEventsService = services.recordEventsService
    }

    @Test
    fun test() {

        typesInfo[TEST_TYPE_ID] = TypeInfo.create {
            withModel(
                TypeModelDef.create {
                    withAttributes(
                        listOf(
                            AttributeDef.create {
                                withId("field0")
                            },
                            AttributeDef.create {
                                withId("field1")
                                withType(AttributeType.NUMBER)
                            }
                        )
                    )
                }
            )
        }

        val events = mutableListOf<EventToListen>()
        services.eventsService.addListener {
            withEventType(RecordChangedEvent.TYPE)
            withDataClass(EventToListen::class.java)
            withAction { events.add(it) }
        }

        val before = RecordValue("abc", 0f, "unknownField-123")
        val after = RecordValue("def", 10f, "unknownField-456")

        recEventsService.emitRecChanged(before, after)

        assertThat(events).containsExactly(
            EventToListen(
                listOf(
                    ChangedValue("field0", DataValue.create("abc"), DataValue.create("def")),
                    ChangedValue("field1", DataValue.create(0.0), DataValue.create(10.0))
                )
            )
        )
    }

    @Test
    fun statusChangedTest() {

        val record = mapOf("abc" to "def")
        val statusBefore = StatusDef.create()
            .withId("draft")
            .withName(MLText("Draft"))
            .build()
        val statusAfter = StatusDef.create()
            .withId("confirm")
            .withName(MLText("Confirm"))
            .build()
        val typeDef = TypeInfo.create()
            .withId("test-type")
            .withModel(TypeModelDef.create()
                .withStatuses(listOf(statusBefore, statusAfter))
                .build())
            .build()


        val events = mutableListOf<DataValue>()
        services.eventsService.addListener {
            withEventType(RecordStatusChangedEvent.TYPE)
            withDataClass(DataValue::class.java)
            withAttributes(mapOf(
                "before" to "before",
                "before.id" to "before.id",
                "before.name" to "before.name",
                "before?str" to "before?str",
                "before?disp" to "before?disp",
                "after" to "after",
                "after.id" to "after.id",
                "after.name" to "after.name",
                "after?str" to "after?str",
                "after?disp" to "after?disp"
            ))
            withAction {
                events.add(it)
            }
        }

        recEventsService.emitRecStatusChanged(RecordStatusChangedEvent(record, typeDef, statusBefore, statusAfter))

        Thread.sleep(100)

        println(events)

        assertThat(events).hasSize(1)
        assertThat(events[0]["before"].asText()).isEqualTo("draft")
        assertThat(events[0]["before.id"].asText()).isEqualTo("draft")
        assertThat(events[0]["before.name"].asText()).isEqualTo("Draft")
        assertThat(events[0]["after"].asText()).isEqualTo("confirm")
        assertThat(events[0]["after.id"].asText()).isEqualTo("confirm")
        assertThat(events[0]["after.name"].asText()).isEqualTo("Confirm")
    }

    data class EventToListen(
        @AttName("diff.list[]")
        val diff: List<ChangedValue>
    )

    data class ChangedValue(
        val id: String,
        val before: DataValue,
        val after: DataValue
    )

    class RecordValue(
        val field0: String,
        val field1: Float,
        val unknownField: String
    ) {
        fun getEcosType() = TEST_TYPE_ID
    }
}
