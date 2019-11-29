package ru.citeck.ecos.events.data.dto.record;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Roman Makarskiy
 */
@Data
public class Attribute {

    private String name;
    private List<Map<String, String>> values = new ArrayList<>();

}
