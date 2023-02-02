package ru.citeck.ecos.events2.txn

import ru.citeck.ecos.events2.EcosEvent

class RemoteEventTxnAction(
    val targetAppKey: String,
    val event: EcosEvent
)
