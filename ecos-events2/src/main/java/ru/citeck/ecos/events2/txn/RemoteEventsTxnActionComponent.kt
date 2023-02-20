package ru.citeck.ecos.events2.txn

import mu.KotlinLogging
import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.remote.AppKeyUtils
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.records3.txn.ext.TxnActionComponent
import ru.citeck.ecos.txn.lib.TxnContext

@Deprecated("Legacy transaction system for events")
class RemoteEventsTxnActionComponent(services: EventsServiceFactory) : TxnActionComponent<RemoteEventTxnAction> {

    companion object {
        const val ID = "event"

        private const val AFTER_COMMIT_EVENTS_KEY = "__after_commit_events__"

        private val log = KotlinLogging.logger {}
    }

    private val remoteEvents = services.remoteEventsService
    private val eventsService = services.eventsService

    private val currentAppName: String
    private val currentAppInstanceId: String

    private val controllerByType = services.remoteEventControllers.associateBy { it.getType() }

    init {
        val props = services.recordsServices.webappProps
        currentAppName = props.appName
        currentAppInstanceId = props.appInstanceId
    }

    private fun mergeEvents(events: List<EcosEvent>): List<EcosEvent> {

        val result = mutableListOf<EcosEvent>()
        if (events.size == 1 || events.isEmpty()) {

            return events
        } else {

            var prevEvent = events[0]

            for (idx in 1 until events.size) {
                var nextEvent = events[idx]
                if (nextEvent.type != prevEvent.type) {
                    result.add(prevEvent)
                } else {
                    val controller = controllerByType[nextEvent.type]
                    if (controller != null && controller.canBeMerged(prevEvent, nextEvent)) {
                        nextEvent = controller.merge(prevEvent, nextEvent)
                    } else {
                        result.add(prevEvent)
                    }
                }
                prevEvent = nextEvent
            }
            result.add(prevEvent)
        }

        return result
    }

    override fun execute(actions: List<RemoteEventTxnAction>) {
        groupByTargetApp(actions) { targetAppKey, events ->
            if (AppKeyUtils.isKeyForApp(currentAppName, currentAppInstanceId, targetAppKey)) {
                val mergedEvents = mergeEvents(events)
                mergedEvents.forEach { eventToEmit ->
                    TxnContext.processListAfterCommit(
                        "legacy-txn-action-component-events-after-commit",
                        {
                            eventsService.emitEventFromRemote(
                                eventToEmit,
                                AppKeyUtils.isKeyExclusive(targetAppKey),
                                false
                            )
                        }
                    ) { elements ->
                        elements.forEach {
                            try {
                                it.invoke()
                            } catch (e: Throwable) {
                                log.error(e) {
                                    "Error in after-commit event ${eventToEmit.id} with type ${eventToEmit.type}"
                                }
                            }
                        }
                    }
                }
            } else {
                val context = RequestContext.getCurrent()
                if (context == null || context.ctxData.txnId == null) {
                    sendRemoteEvents(targetAppKey, mergeEvents(events))
                } else {
                    val eventsByTargetApp = context.getMap<String, MutableList<EcosEvent>>(AFTER_COMMIT_EVENTS_KEY)
                    if (eventsByTargetApp.isEmpty()) {
                        context.doAfterCommit {
                            val eventsByTargetAppCopy = LinkedHashMap(eventsByTargetApp)
                            eventsByTargetApp.clear()
                            eventsByTargetAppCopy.forEach { (ctxTargetApp, ctxEvents) ->
                                sendRemoteEvents(ctxTargetApp, mergeEvents(ctxEvents))
                            }
                        }
                    }
                    eventsByTargetApp.computeIfAbsent(targetAppKey) { ArrayList() }.addAll(events)
                }
            }
        }
    }

    private inline fun groupByTargetApp(
        actions: List<RemoteEventTxnAction>,
        action: (String, List<EcosEvent>) -> Unit
    ) {
        val eventsByTargetApp = mutableMapOf<String, MutableList<EcosEvent>>()
        actions.forEach {
            eventsByTargetApp.computeIfAbsent(it.targetAppKey) { ArrayList() }.add(it.event)
        }
        eventsByTargetApp.forEach { (targetAppKey, appActions) ->
            action.invoke(targetAppKey, appActions)
        }
    }

    private fun sendRemoteEvents(targetAppKey: String, events: List<EcosEvent>) {
        for (event in events) {
            remoteEvents?.emitEvent(targetAppKey, event, false)
        }
    }

    override fun getType() = ID
}
