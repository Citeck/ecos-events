package ru.citeck.ecos.events2.txn

import ru.citeck.ecos.events2.EcosEvent

class RemoteEventsTxnAction(
    val target: String,
    val event: EcosEvent
)