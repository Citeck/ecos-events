package ru.citeck.ecos.events2

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.events2.type.RecordChangedEvent
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
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

    @Test
    fun test() {

        val typesInfo = HashMap<String, TypeInfo>()

        val services = EventsServiceFactory()
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

        val recEventsService = services.recordEventsService

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
        services.eventsService.addListener<EventToListen> {
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
