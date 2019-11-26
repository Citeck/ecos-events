package ru.citeck.ecos.events.data.dto.record;

import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.citeck.ecos.events.data.dto.EventCommonData;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Roman Makarskiy
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class RecordEventDto extends EventCommonData {

    private List<AttrChanged> changes = new ArrayList<>();

}
