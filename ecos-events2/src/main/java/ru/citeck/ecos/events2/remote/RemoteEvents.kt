package ru.citeck.ecos.events2.remote

import ru.citeck.ecos.events2.EcosEvent

interface RemoteEvents {

    fun doWithListeners(action: (String, List<RemoteListener>) -> Unit)

    fun listenEvents(events: Map<String, Set<String>>)

    fun emitEvent(target: String, event: EcosEvent)

    fun addProducedEventType(eventType: String)
}