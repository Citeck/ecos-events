package ru.citeck.ecos.events2.txn

import ru.citeck.ecos.events2.EcosEvent

class RemoteEventTxnAction(
    val target: String,
    val event: EcosEvent
)