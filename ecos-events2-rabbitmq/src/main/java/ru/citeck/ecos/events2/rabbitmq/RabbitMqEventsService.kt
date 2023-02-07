package ru.citeck.ecos.events2.rabbitmq

import com.rabbitmq.client.BuiltinExchangeType
import ecos.curator.org.apache.zookeeper.Watcher
import mu.KotlinLogging
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.remote.*
import ru.citeck.ecos.events2.web.EmitEventWebExecutor
import ru.citeck.ecos.rabbitmq.RabbitMqChannel
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.zookeeper.EcosZooKeeper
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class RabbitMqEventsService(
    rabbitMqConnection: RabbitMqConn,
    private val factory: EventsServiceFactory,
    ecosZooKeeper: EcosZooKeeper,
    webAppApi: EcosWebAppApi
) : RemoteEventsService {

    companion object {
        val log = KotlinLogging.logger {}

        private const val EVENTS_EXCHANGE = "ecos-events"

        private val EVENT_TYPE_ESCAPER = NameUtils.getEscaperWithAllowedChars("-.")
    }

    private var onRemoteListenersChanged: (String, List<RemoteAppEventListener>) -> Unit = { _, _ -> }
    private var eventListeners: Map<RemoteEventListenerKey, RemoteEventListenerData> = emptyMap()
    private var eventListenersInitialized = false

    private lateinit var outcomeChannel: RabbitMqChannel

    private val producedEventTypes: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap())
    private val remoteListeners: MutableMap<String, List<RemoteAppEventListener>> = ConcurrentHashMap()

    private val appName: String
    private val appInstanceId: String

    private val ecosZooKeeper = ecosZooKeeper.withNamespace("ecos/events")
    private val webClient = webAppApi.getWebClientApi()

    init {

        val recProps = factory.recordsServices.webappProps
        appName = recProps.appName
        appInstanceId = recProps.appInstanceId

        val exclusiveTargetAppKey = AppKeyUtils.createKey(appName, appInstanceId, true)
        val inclusiveTargetAppKey = AppKeyUtils.createKey(appName, appInstanceId, false)

        if (recProps.appName.isBlank()) {

            log.warn { "App name is blank. Remote events listeners won't be registered" }
        } else {

            // events should be consumed in the same order as it
            // occurred and multiple consumers should not be registered
            rabbitMqConnection.doWithNewChannel { channel ->

                outcomeChannel = channel
                channel.declareExchange(
                    EVENTS_EXCHANGE,
                    BuiltinExchangeType.TOPIC,
                    true
                )
                channel.declareQueue(exclusiveTargetAppKey, true)
                channel.queueBind(exclusiveTargetAppKey, EVENTS_EXCHANGE, exclusiveTargetAppKey)

                channel.addAckedConsumer(exclusiveTargetAppKey, EcosEvent::class.java) { event, _ ->
                    onEventReceived(event.getContent(), true)
                }

                channel.declareQueue(inclusiveTargetAppKey, false)
                channel.queueBind(inclusiveTargetAppKey, EVENTS_EXCHANGE, inclusiveTargetAppKey)

                channel.addAckedConsumer(inclusiveTargetAppKey, EcosEvent::class.java) { event, _ ->
                    onEventReceived(event.getContent(), false)
                }
            }
        }
    }

    override fun listenListenersChange(action: (String, List<RemoteAppEventListener>) -> Unit) {
        this.onRemoteListenersChanged = action
    }

    override fun addProducedEventType(eventType: String) {
        if (producedEventTypes.add(eventType)) {
            updateRemoteListeners(eventType)
            ecosZooKeeper.watchChildrenRecursive("/${getZkKeyForEventType(eventType)}") {
                val type = it.type
                if (type != null) {
                    when (type) {
                        Watcher.Event.EventType.NodeDataChanged,
                        Watcher.Event.EventType.NodeCreated,
                        Watcher.Event.EventType.NodeDeleted,
                        Watcher.Event.EventType.NodeChildrenChanged -> {
                            updateRemoteListeners(eventType)
                        }
                        else -> {}
                    }
                }
            }
        }
    }

    private fun updateRemoteListeners(eventType: String) {

        val children = ecosZooKeeper.getChildren("/${getZkKeyForEventType(eventType)}")

        val listeners = mutableListOf<RemoteAppEventListener>()
        children.forEach { targetAppKey ->

            if (!AppKeyUtils.isKeyForApp(appName, appInstanceId, targetAppKey)) {

                val appListener = ecosZooKeeper.getValue(
                    "/${getZkKeyForEventType(eventType)}/$targetAppKey",
                    ZkAppEventListener::class.java
                )
                if (appListener != null) {
                    listeners.add(
                        RemoteAppEventListener(
                            targetAppKey,
                            appListener.attributes,
                            appListener.filter,
                            appListener.transactional
                        )
                    )
                }
            }
        }
        if (listeners.isNotEmpty()) {
            remoteListeners[eventType] = listeners
            onRemoteListenersChanged.invoke(eventType, listeners)
        } else if (remoteListeners.contains(eventType)) {
            remoteListeners.remove(eventType)
            onRemoteListenersChanged.invoke(eventType, emptyList())
        }
    }

    override fun listenEventsFromRemote(listeners: Map<RemoteEventListenerKey, RemoteEventListenerData>) {

        val newListeners = HashMap(listeners)
        if (eventListenersInitialized && eventListeners == newListeners) {
            return
        }

        val listenersToRemove = mutableSetOf<RemoteEventListenerKey>()
        eventListeners.filter { (key, _) ->
            !newListeners.containsKey(key)
        }.forEach { (key, _) ->
            listenersToRemove.add(key)
        }
        if (!eventListenersInitialized) {
            getCurrentAppListenersFromZk().forEach {
                if (!newListeners.containsKey(it)) {
                    listenersToRemove.add(it)
                }
            }
        }
        listenersToRemove.forEach {
            val targetAppKey = AppKeyUtils.createKey(appName, appInstanceId, it.exclusive)
            ecosZooKeeper.setValue("/${getZkKeyForEventType(it.eventType)}/$targetAppKey", null)
        }

        newListeners.forEach { (listenerKey, listenerData) ->
            val targetAppKey = AppKeyUtils.createKey(appName, appInstanceId, listenerKey.exclusive)
            val zkListener = ZkAppEventListener(
                listenerData.attributes,
                listenerData.filter,
                listenerData.transactional
            )
            val valuePath = "/${getZkKeyForEventType(listenerKey.eventType)}/$targetAppKey"
            val exclusiveMsg = if (listenerKey.exclusive) {
                "exclusive"
            } else {
                "inclusive"
            }

            log.info { "Add $exclusiveMsg ZkListener $zkListener for path $valuePath" }
            ecosZooKeeper.setValue(valuePath, zkListener, persistent = listenerKey.exclusive)
        }

        eventListeners = newListeners
        eventListenersInitialized = true
    }

    private fun getCurrentAppListenersFromZk(): List<RemoteEventListenerKey> {
        val currentAppKey = AppKeyUtils.createKey(appName, appInstanceId, true)
        val eventTypes = ecosZooKeeper.getChildren("/")
        val result = mutableListOf<RemoteEventListenerKey>()
        for (eventTypeZkKey in eventTypes) {
            val value = ecosZooKeeper.getValue(
                "/$eventTypeZkKey/$currentAppKey",
                ZkAppEventListener::class.java
            )
            if (value != null) {
                result.add(RemoteEventListenerKey(getEventTypeFromZkKey(eventTypeZkKey), true))
            }
        }
        return result
    }

    override fun emitEvent(targetAppKey: String, event: EcosEvent, transactional: Boolean) {
        if (transactional) {
            webClient.newRequest()
                .targetApp(AppKeyUtils.getAppName(targetAppKey))
                .path(EmitEventWebExecutor.PATH)
                .body { it.writeDto(EmitEventWebExecutor.Body(event)) }
                .executeSync {}
        } else {
            outcomeChannel.publishMsg(EVENTS_EXCHANGE, targetAppKey, event)
        }
    }

    private fun getZkKeyForEventType(type: String): String {
        return EVENT_TYPE_ESCAPER.escape(type)
    }

    private fun getEventTypeFromZkKey(key: String): String {
        return EVENT_TYPE_ESCAPER.unescape(key)
    }

    private fun onEventReceived(event: EcosEvent, exclusive: Boolean) {
        if (event.user.isNotBlank()) {
            AuthContext.runAsFull(event.user) {
                onEventReceivedImpl(event, exclusive)
            }
        } else {
            onEventReceivedImpl(event, exclusive)
        }
    }

    private fun onEventReceivedImpl(event: EcosEvent, exclusive: Boolean) {
        AuthContext.runAsSystem {
            // without this try/catch first exception lead to consumer death
            try {
                TxnContext.doInNewTxn {
                    RequestContext.doWithCtx {
                        factory.eventsService.emitEventFromRemote(event, exclusive, false)
                    }
                }
            } catch (e: Exception) {
                log.error(e) {
                    "Exception while event processing. " +
                        "Event id: ${event.id} source: ${event.source} type: ${event.type}"
                }
            }
        }
    }
}
