package ru.citeck.ecos.events2.obs

import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.listener.ctx.ListenerInfo
import ru.citeck.ecos.micrometer.obs.EcosObsContext

sealed class EventsObsCtx(
    name: String,
    val event: EcosEvent
) : EcosObsContext(name) {

    class RemoteDispatch(
        event: EcosEvent,
        val exclusive: Boolean,
        val calledInSameTxn: Boolean
    ) : EventsObsCtx("ecos.events.remote.dispatch", event)

    class Listener(
        event: EcosEvent,
        val listener: ListenerInfo,
        val phase: Phase
    ) : EventsObsCtx(phase.observationName, event)

    enum class Phase(val observationName: String) {
        PREPARE("ecos.events.listener"),
        FILTER("ecos.events.listener.filter"),
        CONVERT("ecos.events.listener.convert"),
        ACTION("ecos.events.listener.action")
    }
}
