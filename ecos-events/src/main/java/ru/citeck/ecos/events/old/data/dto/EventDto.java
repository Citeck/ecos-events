package ru.citeck.ecos.events.old.data.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import lombok.Data;

/**
 * @author Roman Makarskiy
 */
@Data
public class EventDto {

    protected JsonNode data = NullNode.getInstance();

    public String resolveType() {
        return data.get("type").asText();
    }

}
