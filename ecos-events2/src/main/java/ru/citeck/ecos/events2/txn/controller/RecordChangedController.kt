package ru.citeck.ecos.events2.txn.controller

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.type.RecordChangedEvent

class RecordChangedController : RemoteEventController {

    override fun canBeMerged(event0: EcosEvent, event1: EcosEvent): Boolean {

        val id0 = getRecordId(event0)
        val id1 = getRecordId(event1)

        return id0.isNotEmpty() && id0 == id1 && event0.user == event1.user
    }

    override fun merge(event0: EcosEvent, event1: EcosEvent): EcosEvent {

        val newAttributes = event0.attributes.deepCopy()

        event1.attributes.forEach { k, v ->
            if (!newAttributes.has(k)) {
                newAttributes.set(k, v)
            } else if (!k.startsWith("before")) {
                if (k.startsWith("diff._has")) {
                    val old = newAttributes.get(k)
                    newAttributes.set(k, old.asBoolean() || v.asBoolean())
                } else if (k.startsWith("diff.list[")) {
                    newAttributes.set(k, mergeDiffList(newAttributes.get(k), v))
                } else {
                    newAttributes.set(k, v)
                }
            }
        }

        return EcosEvent(
            event1.id,
            event1.time,
            getType(),
            event1.user,
            event1.source,
            newAttributes
        )
    }

    private fun mergeDiffList(before: DataValue, after: DataValue): DataValue {
        if (!before.isArray()) {
            return after
        }
        if (!after.isArray()) {
            return before
        }
        val attById = linkedMapOf<String, DataValue>()
        before.forEach { attById[it.get("id").asText()] = it }
        after.forEach { newAtt ->
            val id = newAtt.get("id").asText()
            val oldAtt = attById[id]
            if (oldAtt == null) {
                attById[id] = newAtt
            } else {
                val copy = oldAtt.copy()
                newAtt.forEach { k, v ->
                    if (k.startsWith("after")) {
                        copy.set(k, v)
                    }
                }
                attById[id] = copy
            }
        }
        return DataValue.create(attById.values.toList())
    }

    private fun getRecordId(event: EcosEvent): String {
        val id = event.attributes.get("record?id")
        if (id.isNotNull()) {
            return id.asText()
        }
        return ""
    }

    override fun getType() = RecordChangedEvent.TYPE
}