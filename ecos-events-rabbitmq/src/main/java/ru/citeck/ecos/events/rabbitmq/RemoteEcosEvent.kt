package ru.citeck.ecos.events.rabbitmq

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.events.EcosEvent

class RemoteEcosEvent(
    val event: EcosEvent,
    val data: ObjectData
)