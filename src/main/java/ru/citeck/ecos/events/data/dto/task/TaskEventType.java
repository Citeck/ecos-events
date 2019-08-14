package ru.citeck.ecos.events.data.dto.task;

import lombok.RequiredArgsConstructor;

/**
 * @author Roman Makarskiy
 */
@RequiredArgsConstructor
public enum TaskEventType {

    CREATE("task.create"),
    ASSIGN("task.assign"),
    COMPLETE("task.complete"),
    DELETE("task.delete");

    private final String type;

    @Override
    public String toString() {
        return this.type;
    }

    public static TaskEventType resolve(String type) {
        for (TaskEventType current : TaskEventType.values()) {
            if (current.type.equals(type)) {
                return current;
            }
        }

        throw new IllegalArgumentException(String.format("TaskEventType with type <%s> not found", type));
    }
}
