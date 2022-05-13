package ru.citeck.ecos.events2

import ru.citeck.ecos.events2.listener.ctx.ListenersContext
import ru.citeck.ecos.events2.remote.RemoteEventsService
import ru.citeck.ecos.events2.txn.RemoteEventsTxnActionComponent
import ru.citeck.ecos.events2.txn.controller.RecordChangedController
import ru.citeck.ecos.events2.txn.controller.RemoteEventController
import ru.citeck.ecos.events2.type.RecordEventsService
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.records3.RecordsServiceFactory

open class EventsServiceFactory {

    val eventsService: EventsService by lazy { createEventsService() }
    val remoteEventsService: RemoteEventsService? by lazy { createRemoteEvents() }
    val listenersContext: ListenersContext by lazy { createListenersContext() }
    val properties: EventsProperties by lazy { createProperties() }
    val remoteEventControllers: List<RemoteEventController> by lazy { createRemoteEventControllers() }
    val recordEventsService: RecordEventsService by lazy { createRecordEventsService() }

    lateinit var recordsServices: RecordsServiceFactory
    lateinit var modelServices: ModelServiceFactory

    open fun init() {
        val remoteEventsExecutor = RemoteEventsTxnActionComponent(this)
        recordsServices.txnActionManager.register(remoteEventsExecutor)
    }

    open fun createRecordEventsService(): RecordEventsService {
        return RecordEventsService(this)
    }

    open fun createRemoteEventControllers(): List<RemoteEventController> {
        return listOf(
            RecordChangedController()
        )
    }

    open fun createProperties(): EventsProperties {
        return EventsProperties()
    }

    open fun createListenersContext(): ListenersContext {
        return ListenersContext(this)
    }

    open fun createRemoteEvents(): RemoteEventsService? {
        return null
    }

    open fun createEventsService(): EventsService {
        return EventsServiceImpl(this)
    }
}
