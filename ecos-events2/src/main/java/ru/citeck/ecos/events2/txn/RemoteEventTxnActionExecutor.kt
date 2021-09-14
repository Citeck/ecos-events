package ru.citeck.ecos.events2.txn

import ru.citeck.ecos.events2.EventConstants
import ru.citeck.ecos.events2.EventServiceFactory
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.records3.txn.ext.TxnActionExecutor

class RemoteEventTxnActionExecutor(services: EventServiceFactory) : TxnActionExecutor<RemoteEventTxnAction> {

    companion object {
        const val ID = "event"

        private const val AFTER_COMMIT_EVENTS_KEY = "__after_commit_events__"
    }

    private val remoteEvents = services.remoteEvents
    private val eventService = services.eventService

    private val appName: String
    private val appInstanceTarget: String

    init {
        val props = services.recordsServices.properties
        appName = props.appName
        appInstanceTarget = EventConstants.REMOTE_TARGET_INSTANCE_PREFIX + props.appInstanceId
    }

    override fun execute(action: RemoteEventTxnAction) {
        if (action.target == appName || action.target == appInstanceTarget) {
            eventService.emitRemoteEvent(action.event)
        } else {
            val context = RequestContext.getCurrent()
            if (context == null) {
                sendRemoteEvent(action)
            } else {
                val events = context.getList<RemoteEventTxnAction>(AFTER_COMMIT_EVENTS_KEY)
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

    private fun sendRemoteEvent(action: RemoteEventTxnAction) {
        remoteEvents?.emitEvent(action.target, action.event)
    }

    override fun getType() = ID
}