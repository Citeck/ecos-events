package ru.citeck.ecos.events.data.dto.pasrse;

import ru.citeck.ecos.events.data.dto.EventDto;

/**
 * @author Roman Makarskiy
 */
public class UnsupportedEventTypeException extends RuntimeException {

    public UnsupportedEventTypeException(String type, EventDto dto) {
        super(String.format("Type <%s> not supported. Dto: <%s>", type, dto));
    }

}
