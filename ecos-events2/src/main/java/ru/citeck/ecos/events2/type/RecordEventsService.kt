package ru.citeck.ecos.events2.type

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.events2.EventsServiceFactory
import ru.citeck.ecos.events2.emitter.EmitterConfig
import ru.citeck.ecos.events2.emitter.EventsEmitter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttSchemaResolver
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.webapp.api.entity.EntityRef

class RecordEventsService(services: EventsServiceFactory) {

    private var recChangedEmitter: EventsEmitter<RecordChangedEvent>
    private var recCreatedEmitter: EventsEmitter<RecordCreatedEvent>
    private var recStatusChangedEmitter: EventsEmitter<RecordStatusChangedEvent>
    private var recDraftStatusChangedEmitter: EventsEmitter<RecordDraftStatusChangedEvent>
    private var recDeletedEmitter: EventsEmitter<RecordDeletedEvent>

    private val typesRepo: TypesRepo = services.modelServices.typesRepo
    private val records: RecordsService = services.recordsServices.recordsService

    init {
        val eventsService = services.eventsService

        recChangedEmitter = eventsService.getEmitter(
            EmitterConfig.create {
                withEventType(RecordChangedEvent.TYPE)
                withSource(RecordChangedEvent::class.java.simpleName)
                withEventClass(RecordChangedEvent::class.java)
            }
        )
        recCreatedEmitter = eventsService.getEmitter(
            EmitterConfig.create {
                withEventType(RecordCreatedEvent.TYPE)
                withSource(RecordCreatedEvent::class.java.simpleName)
                withEventClass(RecordCreatedEvent::class.java)
            }
        )
        recStatusChangedEmitter = eventsService.getEmitter(
            EmitterConfig.create {
                withEventType(RecordStatusChangedEvent.TYPE)
                withSource(RecordStatusChangedEvent::class.java.simpleName)
                withEventClass(RecordStatusChangedEvent::class.java)
            }
        )
        recDeletedEmitter = eventsService.getEmitter(
            EmitterConfig.create {
                withEventType(RecordDeletedEvent.TYPE)
                withSource(RecordDeletedEvent::class.java.simpleName)
                withEventClass(RecordDeletedEvent::class.java)
            }
        )
        recDraftStatusChangedEmitter = eventsService.getEmitter(
            EmitterConfig.create {
                withEventType(RecordDraftStatusChangedEvent.TYPE)
                withSource(RecordDraftStatusChangedEvent::class.java.simpleName)
                withEventClass(RecordDraftStatusChangedEvent::class.java)
            }
        )
    }

    @JvmOverloads
    fun <T : Any> emitRecChanged(before: T?, after: T, sourceId: String = "", mapToRec: (T) -> Any? = { it }) {
        RequestContext.doWithCtx { context: RequestContext ->
            AuthContext.runAsSystem {
                if (sourceId.isEmpty()) {
                    emitRecChangedInCtx(before, after, mapToRec)
                } else {
                    context.doWithVar(AttSchemaResolver.CTX_SOURCE_ID_KEY, sourceId) {
                        emitRecChangedInCtx(before, after, mapToRec)
                    }
                }
            }
        }
    }

    private fun <T : Any> emitRecChangedInCtx(before: T?, after: T, mapToRec: (T) -> Any?) {

        val beforeRec = before?.let { mapToRec(it) }
        val afterRec = mapToRec.invoke(after)

        val typeInfo = afterRec?.let { getTypeInfoFromRecord(it) } ?: return

        if (beforeRec == null) {
            emitRecCreated(afterRec, typeInfo)
            return
        }

        val attsToRequest = mutableMapOf<String, String>()
        for (att in typeInfo.model.attributes) {
            var attStr = att.id
            if (att.multiple) {
                attStr += "[]"
            }
            attStr += ScalarType.RAW.schema
            attsToRequest[att.id] = attStr
        }
        val beforeAtts = getAtts(beforeRec, attsToRequest)
        val afterAtts = getAtts(afterRec, attsToRequest)

        if (beforeAtts == afterAtts) {
            return
        }

        emitRecChanged(RecordChangedEvent(afterRec, typeInfo, beforeAtts, afterAtts, emptyList(), false))
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

    @JvmOverloads
    fun emitRecChanged(record: Any, before: Map<String, Any?>, after: Map<String, Any?>, isDraft: Boolean = false) {
        val typeInfo = getTypeInfoFromRecord(record) ?: return
        emitRecChanged(RecordChangedEvent(record, typeInfo, before, after, emptyList(), isDraft))
    }

    fun emitRecChanged(event: RecordChangedEvent) {
        recChangedEmitter.emit(event)
    }

    @JvmOverloads
    fun emitRecCreated(record: Any, typeInfo: TypeInfo? = null, isDraft: Boolean = false) {

        val nnTypeInfo = typeInfo ?: getTypeInfoFromRecord(record) ?: return

        emitRecCreated(
            RecordCreatedEvent(record, nnTypeInfo, isDraft) {
                val attsToRequest = mutableMapOf<String, String>()
                val attsById = nnTypeInfo.model.attributes.associateBy { it.id }
                for ((id, def) in attsById) {
                    if (isAssocLikeAttribute(def.type)) {
                        attsToRequest[id] = "$id[]${ScalarType.ID.schema}"
                    }
                }
                val assocsValues = records.getAtts(record, attsToRequest).getAtts()
                val assocsInfo = ArrayList<RecordCreatedEvent.AssocInfo>()
                assocsValues.forEach { id, values ->
                    if (values.isArray() && values.isNotEmpty()) {
                        val def = attsById[id]
                        if (def != null) {
                            val isChild = def.config.get("child", false)
                            assocsInfo.add(
                                RecordCreatedEvent.AssocInfo(
                                    id,
                                    def,
                                    isChild,
                                    values.asList(EntityRef::class.java)
                                )
                            )
                        }
                    }
                }
                assocsInfo
            }
        )
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
        return typesRepo.getTypeInfo(ModelUtils.getTypeRef(typeId))
    }

    // todo: move this logic to ecos-model-lib
    private fun isAssocLikeAttribute(type: AttributeType?): Boolean {
        type ?: return false
        return type == AttributeType.ASSOC ||
            type == AttributeType.PERSON ||
            type == AttributeType.AUTHORITY_GROUP ||
            type == AttributeType.AUTHORITY
    }
}
