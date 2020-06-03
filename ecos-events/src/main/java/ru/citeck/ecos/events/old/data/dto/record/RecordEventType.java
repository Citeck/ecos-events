package ru.citeck.ecos.events.old.data.dto.record;

import lombok.RequiredArgsConstructor;

/**
 * @author Roman Makarskiy
 */
@RequiredArgsConstructor
public enum RecordEventType {

    CREATE("record.create"),
    UPDATE("record.update");

    private final String type;

    @Override
    public String toString() {
        return this.type;
    }

    public static RecordEventType resolve(String type) {
        for (RecordEventType current : RecordEventType.values()) {
            if (current.type.equals(type)) {
                return current;
            }
        }

        throw new IllegalArgumentException(String.format("RecordEventType with type <%s> not found", type));
    }

}
