package ru.citeck.ecos.events.data.dto.pasrse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import ru.citeck.ecos.events.data.dto.EventDto;
import ru.citeck.ecos.events.data.dto.record.RecordEventDto;
import ru.citeck.ecos.events.data.dto.task.TaskEventDto;

import java.io.IOException;

/**
 * @author Roman Makarskiy
 */
public class EventDtoFactory {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> T fromEventDtoMsg(@NonNull String content) {
        try {
            EventDto eventDto = OBJECT_MAPPER.readValue(content, EventDto.class);
            return fromEventDto(eventDto);
        } catch (IOException e) {
            throw new ParseEventDtoException(content, e);
        }
    }

    public static <T> T fromEventDto(@NonNull EventDto eventDto) {
        String type = eventDto.resolveType();

        if (StringUtils.startsWith(type, "task.")) {
            return castTo(eventDto, TaskEventDto.class);
        } else if (StringUtils.startsWith(type, "record.")) {
            return castTo(eventDto, RecordEventDto.class);
        }

        throw new UnsupportedEventTypeException(type, eventDto);
    }

    public static <T> EventDto toEventDto(T data) {
        JsonNode node = OBJECT_MAPPER.valueToTree(data);
        EventDto eventDto = new EventDto();
        eventDto.setData(node);
        return eventDto;
    }

    @SuppressWarnings("unchecked")
    private static <T> T castTo(EventDto dto, Class clazz) {
        try {
            return (T) OBJECT_MAPPER.treeToValue(dto.getData(), clazz);
        } catch (JsonProcessingException e) {
            throw new ParseEventDtoException(dto, e);
        }
    }

}
