package ru.citeck.ecos.events2

import ru.citeck.ecos.events2.listener.ctx.ListenersContext
import ru.citeck.ecos.events2.remote.RemoteEventsService
import ru.citeck.ecos.events2.type.RecordEventsService
import ru.citeck.ecos.events2.web.TxnEventsWebExecutor
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.records3.RecordsServiceFactory

open class EventsServiceFactory {

    val eventsService: EventsService by lazy { createEventsService() }
    val remoteEventsService: RemoteEventsService? by lazy { createRemoteEvents() }
    val listenersContext: ListenersContext by lazy { createListenersContext() }
    val properties: EventsProperties by lazy { createProperties() }
    val recordEventsService: RecordEventsService by lazy { createRecordEventsService() }

    lateinit var recordsServices: RecordsServiceFactory
    lateinit var modelServices: ModelServiceFactory

    open fun init() {
        recordsServices.getEcosWebAppApi()?.getWebExecutorsApi()?.register(TxnEventsWebExecutor(eventsService))
    }

    open fun createRecordEventsService(): RecordEventsService {
        return RecordEventsService(this)
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
