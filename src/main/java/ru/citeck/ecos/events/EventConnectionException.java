package ru.citeck.ecos.events;

/**
 * @author Roman Makarskiy
 */
public class EventConnectionException extends RuntimeException {
    public EventConnectionException(String message) {
        super(message);
    }

    public EventConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
