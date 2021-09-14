package ru.citeck.ecos.events2

import ru.citeck.ecos.events2.listener.ctx.ListenersContext
import ru.citeck.ecos.events2.remote.RemoteEvents
import ru.citeck.ecos.events2.txn.RemoteEventsTxnActionExecutor
import ru.citeck.ecos.records3.RecordsServiceFactory

open class EventsServiceFactory {

    val eventsService: EventsService by lazy { createEventsService() }
    val remoteEvents: RemoteEvents? by lazy { createRemoteEvents() }
    val listenersContext: ListenersContext by lazy { createListenersContext() }
    val properties: EventsProperties by lazy { createProperties() }

    lateinit var recordsServices: RecordsServiceFactory

    open fun init() {
        val remoteEventsExecutor = RemoteEventsTxnActionExecutor(this)
        recordsServices.txnActionManager.register(remoteEventsExecutor)
    }

    open fun createProperties() : EventsProperties {
        return EventsProperties()
    }

    open fun createListenersContext() : ListenersContext {
        return ListenersContext(this)
    }

    open fun createRemoteEvents() : RemoteEvents? {
        return null
    }

    open fun createEventsService() : EventsService {
        return EventsServiceImpl(this)
    }
}