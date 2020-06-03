package ru.citeck.ecos.events.old.data.dto;

import lombok.Data;

import java.util.Date;

/**
 * @author Roman Makarskiy
 */
@Data
public abstract class EventCommonData {

    protected String id;
    protected String type;
    protected String docId;
    protected Date createdDate;
    protected Long version;

}
