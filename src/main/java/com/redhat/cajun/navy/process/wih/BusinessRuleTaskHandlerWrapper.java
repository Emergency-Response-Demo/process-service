package com.redhat.cajun.navy.process.wih;

import javax.annotation.PostConstruct;

import org.jbpm.process.workitem.bpmn2.BusinessRuleTaskHandler;
import org.jbpm.process.workitem.core.AbstractLogOrThrowWorkItemHandler;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.kie.internal.runtime.Cacheable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("BusinessRuleTask")
public class BusinessRuleTaskHandlerWrapper extends AbstractLogOrThrowWorkItemHandler implements Cacheable {

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
        businessRuleTaskHandler = new BusinessRuleTaskHandler(groupId, artifactId, version, new Long(scannerInterval));
    }

    @Override
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        businessRuleTaskHandler.executeWorkItem(workItem, manager);
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
