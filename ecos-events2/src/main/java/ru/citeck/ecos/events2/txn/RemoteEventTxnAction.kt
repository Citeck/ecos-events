package ru.citeck.ecos.events2.txn

import ru.citeck.ecos.events2.EcosEvent

@Deprecated("Legacy transaction system for events")
class RemoteEventTxnAction(
    val targetAppKey: String,
    val event: EcosEvent
)
