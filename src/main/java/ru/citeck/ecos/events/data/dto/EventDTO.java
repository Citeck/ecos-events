package ru.citeck.ecos.events.data.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import lombok.Data;

/**
 * @author Roman Makarskiy
 */
@Data
public abstract class EventDTO {

    protected String id;
    protected String type;
    protected String docId;
    protected JsonNode additionalData = NullNode.getInstance();

}
