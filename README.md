# ECOS Events

## Event listener observations

The library uses `recordsServices.micrometerContext`; its default remains NOOP.
Contexts retain references to the original `EcosEvent` and `ListenerInfo` objects.
`EventsObsConvention` in ecos-webapp-commons selects the fields exported as tags; it currently exports
only event ID and type, plus listener and delivery metadata. Event payloads, user
identity, source details and filter expressions are not exported as tags.

| Observation | Boundary |
| --- | --- |
| `ecos.events.remote.dispatch` | Remote event listener lookup and dispatch |
| `ecos.events.listener` | One eligible listener, including preparation |
| `ecos.events.listener.filter` | Template resolution and predicate evaluation |
| `ecos.events.listener.convert` | Attribute projection and DTO conversion |
| `ecos.events.listener.action` | Actual callback execution |

`listener.id`, `event.type`, `event.id` and `listener.dataClass` are
high-cardinality trace attributes, not metric dimensions. Set an explicit,
unique listener ID when registering application listeners to make slow callbacks
recognizable. Rejected filters have no conversion/action spans. Deferred local
callbacks create their action observation when they execute after commit, not
when they are queued; they retain the originating event and listener IDs.
Transactional exceptions still propagate, while existing after-commit error
handling remains unchanged. Sampling/export filtering may omit short spans.

Observation metadata is carried by typed `EcosObsContext` classes. Tag policy is
implemented by `GlobalEcosObsConvention`: events in ecos-webapp-commons, BPMN
in ecos-process. The shared convention wrapper controls high-cardinality tags
according to `management.tracing.enabled`. Business code does not add tags directly.
