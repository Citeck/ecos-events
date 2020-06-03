package ru.citeck.ecos.events.old.data.dto.pasrse;

import ru.citeck.ecos.events.old.data.dto.EventDto;

/**
 * @author Roman Makarskiy
 */
public class ParseEventDtoException extends RuntimeException {

    public ParseEventDtoException(EventDto dto, Throwable cause) {
        super(String.format("Failed parse EventDto. Dto: <%s>", dto), cause);
    }

    public ParseEventDtoException(String msg, Throwable cause) {
        super(String.format("Failed parse EventDto from message. Msg: <%s>", msg), cause);
    }

}
