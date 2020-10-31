package com.redhat.cajun.navy.process.wih;

import javax.annotation.PostConstruct;

import com.redhat.cajun.navy.rules.model.Incident;
import com.redhat.cajun.navy.rules.model.Mission;
import org.jbpm.process.workitem.bpmn2.BusinessRuleTaskHandler;
import org.jbpm.process.workitem.core.AbstractLogOrThrowWorkItemHandler;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.kie.internal.runtime.Cacheable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("BusinessRuleTask")
public class BusinessRuleTaskHandlerWrapper extends AbstractLogOrThrowWorkItemHandler implements Cacheable {

    private static Logger log = LoggerFactory.getLogger(BusinessRuleTaskHandlerWrapper.class);

    private BusinessRuleTaskHandler businessRuleTaskHandler;

    @Value("${mission.assignment.rules.groupid}")
    private String groupId;

    @Value(("${mission.assignment.rules.artifactid}"))
    private String artifactId;

    @Value("${mission.assignment.rules.version}")
    private String version;

    @Value("${mission.assignment.rules.scannerinterval}")
    private String scannerInterval;

    @PostConstruct
    public void init() {
        businessRuleTaskHandler = new BusinessRuleTaskHandler(groupId, artifactId, version, Long.parseLong(scannerInterval));
    }

    @Override
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        Incident incident = (Incident) workItem.getParameter("Incident");
        log.debug("Executing assignment rules for incident '" + incident.getId() + "'");
        businessRuleTaskHandler.executeWorkItem(workItem, manager);
        Mission mission = (Mission) workItem.getResult("Mission");
        log.debug("Assignment status for incident '" + incident.getId() + "': " + mission.getStatus().name());
    }

    @Override
    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {
        businessRuleTaskHandler.abortWorkItem(workItem, manager);
    }

    @Override
    public void close() {
        businessRuleTaskHandler.close();
    }
}
