package ru.citeck.ecos.events2.type

import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.emitter.EventsEmitter
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.record.atts.schema.ScalarType

class RecordEventsService(services: EventsServiceFactory) {

    private var recChangedEmitter: EventsEmitter<RecordChangedEvent>
    private var recCreatedEmitter: EventsEmitter<RecordCreatedEvent>
    private var recStatusChangedEmitter: EventsEmitter<RecordStatusChangedEvent>
    private var recDraftStatusChangedEmitter: EventsEmitter<RecordDraftStatusChangedEvent>
    private var recDeletedEmitter: EventsEmitter<RecordDeletedEvent>

    private val typesRepo: TypesRepo = services.modelServices.typesRepo
    private val records: RecordsService = services.recordsServices.recordsServiceV1

    init {
        val eventsService = services.eventsService

        recChangedEmitter = eventsService.getEmitter(EmitterConfig.create {
            withEventType(RecordChangedEvent.TYPE)
            withSource(RecordChangedEvent::class.java.simpleName)
            withEventClass(RecordChangedEvent::class.java)
        })
        recCreatedEmitter = eventsService.getEmitter(EmitterConfig.create {
            withEventType(RecordCreatedEvent.TYPE)
            withSource(RecordCreatedEvent::class.java.simpleName)
            withEventClass(RecordCreatedEvent::class.java)
        })
        recStatusChangedEmitter = eventsService.getEmitter(EmitterConfig.create {
            withEventType(RecordStatusChangedEvent.TYPE)
            withSource(RecordStatusChangedEvent::class.java.simpleName)
            withEventClass(RecordStatusChangedEvent::class.java)
        })
        recDeletedEmitter = eventsService.getEmitter(EmitterConfig.create {
            withEventType(RecordDeletedEvent.TYPE)
            withSource(RecordDeletedEvent::class.java.simpleName)
            withEventClass(RecordDeletedEvent::class.java)
        })
        recDraftStatusChangedEmitter = eventsService.getEmitter(EmitterConfig.create {
            withEventType(RecordDraftStatusChangedEvent.TYPE)
            withSource(RecordDraftStatusChangedEvent::class.java.simpleName)
            withEventClass(RecordDraftStatusChangedEvent::class.java)
        })
    }

    fun emitRecChanged(before: Any?, after: Any) {

        val typeInfo = getTypeInfoFromRecord(after) ?: return

        if (before == null) {
            emitRecCreated(RecordCreatedEvent(after, typeInfo))
            return
        }

        val attsToRequest = mutableMapOf<String, String>()
        for (att in typeInfo.model.attributes) {
            attsToRequest[att.id] = att.id + ScalarType.RAW.schema
        }
        val beforeAtts = getAtts(before, attsToRequest)
        val afterAtts = getAtts(after, attsToRequest)

        if (beforeAtts == afterAtts) {
            return
        }

        emitRecChanged(RecordChangedEvent(after, typeInfo, beforeAtts, afterAtts))
    }

    private fun getAtts(record: Any?, atts: Map<String, String>): Map<String, Any?> {
        @Suppress("UNCHECKED_CAST")
        var result = records.getAtts(record, atts)
            .getAtts()
            .getData()
            .asJavaObj() as? Map<String, Any?>
        if (result == null) {
            result = atts.mapValues { null }
        }
        return result
    }

    fun emitRecChanged(record: Any, before: Map<String, Any?>, after: Map<String, Any?>) {
        val typeInfo = getTypeInfoFromRecord(record) ?: return
        emitRecChanged(RecordChangedEvent(record, typeInfo, before, after))
    }

    fun emitRecChanged(event: RecordChangedEvent) {
        recChangedEmitter.emit(event)
    }

    fun emitRecCreated(record: Any) {
        val typeInfo = getTypeInfoFromRecord(record) ?: return
        emitRecCreated(RecordCreatedEvent(record, typeInfo))
    }

    fun emitRecCreated(event: RecordCreatedEvent) {
        recCreatedEmitter.emit(event)
    }

    fun emitRecStatusChanged(event: RecordStatusChangedEvent) {
        recStatusChangedEmitter.emit(event)
    }

    fun emitRecDeleted(event: RecordDeletedEvent) {
        recDeletedEmitter.emit(event)
    }

    fun emitRecDraftStatusChanged(event: RecordDraftStatusChangedEvent) {
        recDraftStatusChangedEmitter.emit(event)
    }

    private fun getTypeInfoFromRecord(record: Any): TypeInfo? {
        val typeId = records.getAtt(record, RecordConstants.ATT_TYPE + "?localId").asText()
        if (typeId.isBlank()) {
            return null
        }
        return typesRepo.getTypeInfo(TypeUtils.getTypeRef(typeId))
    }
}