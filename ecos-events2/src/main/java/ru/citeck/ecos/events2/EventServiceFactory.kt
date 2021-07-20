package ru.citeck.ecos.events2

import ru.citeck.ecos.events2.listener.ctx.ListenersContext
import ru.citeck.ecos.events2.remote.RemoteEvents
import ru.citeck.ecos.records3.RecordsServiceFactory

open class EventServiceFactory(val recordsServiceFactory: RecordsServiceFactory) {

    val eventService: EventService by lazy { createEventService() }
    val remoteEvents: RemoteEvents? by lazy { createRemoteEvents() }
    val listenersContext: ListenersContext by lazy { createListenersContext() }
    val properties: EventProperties by lazy { createProperties() }

    open fun createProperties() : EventProperties {
        return EventProperties()
    }

    open fun createListenersContext() : ListenersContext {
        return ListenersContext(this)
    }

    open fun createRemoteEvents() : RemoteEvents? {
        return null
    }

    open fun createEventService() : EventService {
        return EventService(this)
    }
}