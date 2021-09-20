package ru.citeck.ecos.events2.type

class RecordCreatedEvent(val rec: Any) {
    companion object {
        const val TYPE = "record-created"
    }
}

class RecordMutatedEvent(val before: Any, val after: Any, val newRecord: Boolean) {
    companion object {
        const val TYPE = "record-mutated"
    }

    fun getRec(): Any {
        return after
    }
}

class RecordDeletedEvent(val rec: Any) {
    companion object {
        const val TYPE = "record-deleted"
    }
}
