package ru.citeck.ecos.events2.remote

import ru.citeck.ecos.events2.EcosEvent

interface RemoteEventsService {

    fun listenListenersChange(action: (String, List<RemoteAppEventListener>) -> Unit)

    fun listenEventsFromRemote(listeners: Map<RemoteEventListenerKey, RemoteEventListenerData>)

    fun emitEvent(targetAppKey: String, event: EcosEvent, transactional: Boolean)

    fun addProducedEventType(eventType: String)
}
