package ru.citeck.ecos.events

import ru.citeck.ecos.events.listener.ctx.ListenersContext
import ru.citeck.ecos.events.remote.RemoteEventService
import ru.citeck.ecos.records2.RecordsServiceFactory

class EventServiceFactory(val recordsServiceFactory: RecordsServiceFactory) {

    val eventService: EventService by lazy { createEventService() }
    val remoteEventService: RemoteEventService by lazy { createRemoteEventService() }
    val listenersContext: ListenersContext by lazy { createListenersContext() }
    val properties: EventProperties by lazy { createProperties() }

    open fun createProperties() : EventProperties {
        return EventProperties()
    }

    open fun createListenersContext() : ListenersContext {
        return ListenersContext(this)
    }

    open fun createRemoteEventService() : RemoteEventService {
        return RemoteEventService(this)
    }

    open fun createEventService() : EventService {
        return EventService(this)
    }
}