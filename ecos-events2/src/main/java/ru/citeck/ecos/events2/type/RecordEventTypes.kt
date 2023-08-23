package ru.citeck.ecos.events2.type

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.json.serialization.annotation.IncludeNonDefault
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

private val EMPTY_ATT_DEF = AttributeDef.create {}

class RecordChangedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    val before: Map<String, Any?>,
    val after: Map<String, Any?>,
    val assocs: List<AssocDiff>,
    val isDraft: Boolean
) {

    companion object {
        const val TYPE = "record-changed"
    }

    fun getDiff(): Any {
        return Diff()
    }

    inner class Diff : AttValue {

        override fun has(name: String): Boolean {
            return !isEqual(after[name], before[name]) || assocs.any { name == it.assocId }
        }

        override fun getAtt(name: String): Any? {
            when (name) {
                "list" -> {
                    val attsDef = typeDef.model.attributes
                    val attsById = attsDef.associateBy { it.id }
                    val res = mutableListOf<DiffValue>()
                    for ((attId, afterValue) in after) {
                        val beforeValue = before[attId]
                        if (!isEqual(afterValue, beforeValue)) {
                            val attDef = attsById[attId] ?: continue
                            res.add(
                                DiffValue(
                                    attId,
                                    attDef,
                                    beforeValue,
                                    afterValue
                                )
                            )
                        }
                    }
                    for (assocDiff in assocs) {
                        val attDef = attsById[assocDiff.assocId] ?: continue
                        res.add(
                            DiffValue(
                                assocDiff.assocId,
                                attDef,
                                added = assocDiff.added,
                                removed = assocDiff.removed
                            )
                        )
                    }
                    return res
                }
            }
            return null
        }

        private fun isEqual(value0: Any?, value1: Any?): Boolean {
            if (isEmpty(value0)) {
                return isEmpty(value1)
            }
            if (value0 == value1) {
                return true
            }
            if (value0 is Number && value1 is Number) {
                return value0.toDouble() == value1.toDouble()
            }
            return false
        }

        private fun isEmpty(value: Any?): Boolean {
            return value == null ||
                value is String && value.isEmpty() ||
                value is DataValue && (
                value.isTextual() && value.asText().isEmpty() ||
                    (value.isArray() || value.isObject()) && value.size() == 0
                )
        }
    }

    @IncludeNonDefault
    class DiffValue(
        val id: String = "",
        val def: AttributeDef = EMPTY_ATT_DEF,
        val before: Any? = null,
        val after: Any? = null,
        val added: List<EntityRef> = emptyList(),
        val removed: List<EntityRef> = emptyList()
    ) {
        override fun toString(): String {
            return Json.mapper.toString(this) ?: "{}"
        }
    }

    class AssocDiff(
        val assocId: String,
        val def: AttributeDef = EMPTY_ATT_DEF,
        val child: Boolean,
        val added: List<EntityRef>,
        val removed: List<EntityRef>,
    )
}

class RecordDeletedEvent(
    val record: Any,
    val typeDef: TypeInfo
) {
    companion object {
        const val TYPE = "record-deleted"
    }
}

class RecordStatusChangedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    before: StatusDef,
    after: StatusDef
) {
    companion object {
        const val TYPE = "record-status-changed"
    }

    private val beforeValue = StatusValue(before)
    private val afterValue = StatusValue(after)

    fun getBefore(): StatusValue {
        return beforeValue
    }

    fun getAfter(): StatusValue {
        return afterValue
    }

    class StatusValue(@AttName("...") val def: StatusDef) {

        @AttName("?str")
        fun getAsStr(): String {
            return def.id
        }

        @AttName("?disp")
        fun getAsDisp(): String {
            return getAsStr()
        }
    }
}

class RecordDraftStatusChangedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    val before: Boolean,
    val after: Boolean
) {
    companion object {
        const val TYPE = "record-draft-status-changed"
    }
}

class RecordCreatedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    val isDraft: Boolean,
    evalAssocs: () -> List<AssocInfo>
) {
    companion object {
        const val TYPE = "record-created"
    }

    constructor(
        record: Any,
        typeDef: TypeInfo,
        isDraft: Boolean,
        assocs: List<AssocInfo>
    ) : this(record, typeDef, isDraft, { assocs })

    val assocs by lazy(evalAssocs)

    class AssocInfo(
        val assocId: String,
        val def: AttributeDef = EMPTY_ATT_DEF,
        val child: Boolean,
        val added: List<EntityRef>
    ) {
        fun getRemoved(): List<EntityRef> {
            return emptyList()
        }
    }
}

class RecordContentChangedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    val before: Any?,
    val after: Any?
) {
    companion object {
        const val TYPE = "record-content-changed"
    }
}
