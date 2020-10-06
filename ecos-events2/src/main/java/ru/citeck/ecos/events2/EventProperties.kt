package ru.citeck.ecos.events2

data class EventProperties(
        var concurrentEventConsumers: Int = 10,
        var appInstanceId: String = "",
        var appName: String = ""
)