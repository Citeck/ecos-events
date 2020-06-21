package ru.citeck.ecos.events.remote

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.events.EcosEvent

class RemoteEcosEvent(
    val event: EcosEvent,
    val attributes: ObjectData
)