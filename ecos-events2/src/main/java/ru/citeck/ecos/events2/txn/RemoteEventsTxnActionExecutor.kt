package ru.citeck.ecos.events2.txn

import ru.citeck.ecos.events2.EventsConstants
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.records3.txn.ext.TxnActionExecutor

class RemoteEventsTxnActionExecutor(services: EventsServiceFactory) : TxnActionExecutor<RemoteEventsTxnAction> {

    companion object {
        const val ID = "event"

        private const val AFTER_COMMIT_EVENTS_KEY = "__after_commit_events__"
    }

    private val remoteEvents = services.remoteEvents
    private val eventsService = services.eventsService

    private val appName: String
    private val appInstanceTarget: String

    init {
        val props = services.recordsServices.properties
        appName = props.appName
        appInstanceTarget = EventsConstants.REMOTE_TARGET_INSTANCE_PREFIX + props.appInstanceId
    }

    override fun execute(action: RemoteEventsTxnAction) {
        if (action.target == appName || action.target == appInstanceTarget) {
            eventsService.emitEventFromRemote(action.event)
        } else {
            val context = RequestContext.getCurrent()
            if (context == null) {
                sendRemoteEvent(action)
            } else {
                val events = context.getList<RemoteEventsTxnAction>(AFTER_COMMIT_EVENTS_KEY)
                if (events.isEmpty()) {
                    context.doAfterCommit {
                        var idx = 0
                        while (idx < events.size) {
                            sendRemoteEvent(events[idx++])
                        }
                    }
                }
                events.add(action)
            }
        }
    }

    private fun sendRemoteEvent(action: RemoteEventsTxnAction) {
        remoteEvents?.emitEvent(action.target, action.event)
    }

    override fun getType() = ID
}