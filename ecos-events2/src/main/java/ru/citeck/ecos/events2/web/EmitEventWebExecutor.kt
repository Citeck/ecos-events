package ru.citeck.ecos.events2.web

import ru.citeck.ecos.events2.EcosEvent
import ru.citeck.ecos.events2.EventsService
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutor
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorReq
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorResp

class EmitEventWebExecutor(private val eventsService: EventsService) : EcosWebExecutor {

    companion object {
        const val PATH = "/events/emit"
    }

    override fun execute(request: EcosWebExecutorReq, response: EcosWebExecutorResp) {
        val body = request.getBodyReader().readDto(Body::class.java)
        eventsService.emitEventFromRemote(
            body.event,
            exclusive = true,
            calledInTxn = true
        )
    }

    override fun getPath(): String {
        return PATH
    }

    override fun isReadOnly(): Boolean {
        return false
    }

    data class Body(
        val event: EcosEvent
    )
}
