package ru.citeck.ecos.events2.txn.controller

import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.type.RecordMutatedEvent

class RecordMutatedController : RemoteEventController {

    override fun canBeMerged(event0: EcosEvent, event1: EcosEvent): Boolean {

        val id0 = getRecordId(event0)
        val id1 = getRecordId(event1)

        return id0.isNotEmpty() && id0 == id1 && event0.user == event1.user
    }

    override fun merge(event0: EcosEvent, event1: EcosEvent): EcosEvent {

        val newAttributes = event0.attributes.deepCopy()

        event1.attributes.forEach { k, v ->
            if (!k.startsWith("before") && !k.startsWith("newRecord")) {
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
        var id = event.attributes.get("after?id")
        if (id.isNotNull()) {
            return id.asText()
        }
        id = event.attributes.get("rec?id")
        if (id.isNotNull()) {
            return id.asText()
        }
        return ""
    }

    override fun getType() = RecordMutatedEvent.TYPE
}