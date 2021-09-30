package ru.citeck.ecos.events2.type

import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
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
            return after[name] != before[name]
        }

        override fun getAtt(name: String): Any? {
            when (name) {
                "list" -> {
                    val attsDef = typeDef.model.attributes
                    val attsById = attsDef.associateBy { it.id }
                    val res = mutableListOf<DiffValue>()
                    for ((attId, afterValue) in after) {
                        val beforeValue = before[attId]
                        if (afterValue != beforeValue) {
                            val attDef = attsById[attId] ?: continue
                            res.add(DiffValue(attId, attDef, beforeValue, afterValue))
                        }
                    }
                    return res
                }
            }
            return null
        }
    }

    class DiffValue(
        val id: String,
        val def: AttributeDef,
        val before: Any?,
        val after: Any?
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
    val before: StatusDef,
    val after: StatusDef
) {
    companion object {
        const val TYPE = "record-status-changed"
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
