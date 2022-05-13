package ru.citeck.ecos.events2.txn.controller

import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.type.RecordChangedEvent

class RecordChangedController : RemoteEventController {

    override fun canBeMerged(event0: EcosEvent, event1: EcosEvent): Boolean {

        val id0 = getRecordId(event0)
        val id1 = getRecordId(event1)

        if (id0.isEmpty() ||
            id0 != id1 ||
            event0.user != event1.user ||
            event0.attributes.size() != event1.attributes.size()
        ) {
            return false
        }

        val names = event0.attributes.fieldNames()
        while (names.hasNext()) {
            val name = names.next()
            if (name.startsWith("diff") || !event1.attributes.has(name)) {
                return false
            }
        }
        return true
    }

    override fun merge(event0: EcosEvent, event1: EcosEvent): EcosEvent {

        val newAttributes = event0.attributes.deepCopy()

        event1.attributes.forEach { k, v ->
            if (!newAttributes.has(k)) {
                newAttributes.set(k, v)
            } else if (!k.startsWith("before")) {
                newAttributes.set(k, v)
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

    private fun getRecordId(event: EcosEvent): String {
        val id = event.attributes.get("record?id")
        if (id.isNotNull()) {
            return id.asText()
        }
        return ""
    }

    override fun getType() = RecordChangedEvent.TYPE
}
