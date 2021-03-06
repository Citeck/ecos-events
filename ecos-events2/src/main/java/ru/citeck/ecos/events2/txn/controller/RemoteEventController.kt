package ru.citeck.ecos.events2.txn.controller

import ru.citeck.ecos.events2.EcosEvent

interface RemoteEventController {

    fun canBeMerged(event0: EcosEvent, event1: EcosEvent): Boolean

    fun merge(event0: EcosEvent, event1: EcosEvent): EcosEvent

    fun getType(): String
}