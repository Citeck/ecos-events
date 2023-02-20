package ru.citeck.ecos.events2.type

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.records3.record.atts.value.AttValue

class RecordChangedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    val before: Map<String, Any?>,
    val after: Map<String, Any?>
) {

    companion object {
        const val TYPE = "record-changed"
    }

    init {
        val atts = typeDef.model.attributes
        if (atts.size != before.size || atts.size != after.size) {
            error("Incorrect atts size. Def: ${atts.size} before: ${before.size} after: ${after.size}")
        }
    }

    fun getDiff(): Any {
        return Diff()
    }

    inner class Diff : AttValue {

        override fun has(name: String): Boolean {
            return !isEqual(after[name], before[name])
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

    class DiffValue(
        val id: String,
        val def: AttributeDef,
        val before: Any?,
        val after: Any?
    ) {
        override fun toString(): String {
            return Json.mapper.toString(this) ?: "{}"
        }
    }
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
    val typeDef: TypeInfo
) {
    companion object {
        const val TYPE = "record-created"
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
