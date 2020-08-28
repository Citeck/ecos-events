package ru.citeck.ecos.events.remote

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.events.EcosEvent

interface RemoteEvents {

    fun doWithListeners(action: (String, List<RemoteListener>) -> Unit)

    fun listenEvents(events: Map<String, Set<String>>)

    fun emitEvent(listener: RemoteListener, event: EcosEvent, data: ObjectData)

    fun addProducedEventType(eventType: String)
}