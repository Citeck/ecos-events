package ru.citeck.ecos.events.data.dto.record;

import lombok.Data;

/**
 * @author Roman Makarskiy
 */
@Data
public class AttrChanged {

    private String attribute;
    private String oldValue;
    private String newValue;

}
