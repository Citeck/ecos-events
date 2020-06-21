package ru.citeck.ecos.events

data class EventProperties(
        var concurrentEventConsumers: Int = 10,
        var appInstanceId: String = "",
        var appName: String = ""
)