package ru.citeck.ecos.events.data.dto.task;

import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.citeck.ecos.events.data.dto.EventDTO;

import java.util.*;

/**
 * @author Roman Makarskiy
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class TaskEventDTO extends EventDTO {

    private Date date;
    private Date dueDate;
    private String taskComment;
    private String taskOutcome;
    private String initiator;
    private String assignee;
    private String document;
    private String taskInstanceId;
    private String taskRole;
    private Set<String> taskPooledActors = new HashSet<>();
    private Set<String> taskAttachments = new HashSet<>();
    private Set<String> taskPooledUsers = new HashSet<>();
    private String workflowInstanceId;
    private String workflowDescription;
    private String taskTitle;
    private String taskType;
    private Integer expectedPerformTime;
    private String caseTask;

}
