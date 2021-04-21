package com.redhat.cajun.navy.process.wih;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import javax.annotation.PostConstruct;

import com.redhat.cajun.navy.process.outbox.OutboxEventEmitter;
import io.cloudevents.CloudEvent;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.api.runtime.process.WorkItemManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("SendMessage")
public class KafkaMessageSenderWorkItemHandler implements WorkItemHandler {

    @Autowired
    private OutboxEventEmitter outboxEventEmitter;

    @Value("${sender.destination.create-mission-command}")
    private String createMissionCommandDestination;

    @Value("${sender.destination.update-responder-command}")
    private String updateResponderCommandDestination;

    @Value("${sender.destination.set-responder-unavailable-command}")
    private String setResponderUnavailableCommandDestination;

    @Value("${sender.destination.update-incident-command}")
    private String updateIncidentCommandDestination;

    @Value("${sender.destination.incident-assignment-event}")
    private String incidentAssignmentEventDestination;

    private final Map<String, Pair<Pair<String, String>, BiFunction<Pair<String, String>, Map<String, Object>, CloudEvent>>> payloadBuilders = new HashMap<>();

    @Override
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        Map<String, Object> parameters = workItem.getParameters();
        Object messageType = parameters.get("MessageType");
        if (!(messageType instanceof String)) {
            throw new IllegalStateException("Parameter 'messageType' cannot be null and must be of type String");
        }
        Pair<Pair<String, String>, BiFunction<Pair<String, String>, Map<String, Object>, CloudEvent>> messagetypeDestinationBuilderTuple = payloadBuilders
                .get(messageType);
        if (messagetypeDestinationBuilderTuple == null) {
            throw new IllegalStateException("No builder found for payload '" + messageType + "'");
        }

        parameters.put("processId", Long.toString(workItem.getProcessInstanceId()));

        CloudEvent cloudEvent = messagetypeDestinationBuilderTuple.getRight()
                .apply(messagetypeDestinationBuilderTuple.getLeft(), parameters);

        send(cloudEvent);
        manager.completeWorkItem(workItem.getId(), Collections.emptyMap());
    }

    private void send(CloudEvent cloudEvent) {
        outboxEventEmitter.emitCloudEvent(cloudEvent);
    }

    @Override
    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {

    }

    @PostConstruct
    public void init() {
        addPayloadBuilder("CreateMission", "CreateMissionCommand", createMissionCommandDestination,
                CreateMissionCommandBuilder::builder);
        addPayloadBuilder("SetResponderUnavailable", "SetResponderUnavailableCommand", setResponderUnavailableCommandDestination,
                SetResponderUnavailableCommandBuilder::builder);
        addPayloadBuilder("UpdateIncident", "UpdateIncidentCommand", updateIncidentCommandDestination,
                UpdateIncidentCommandBuilder::builder);
        addPayloadBuilder("IncidentAssignment", "IncidentAssignmentEvent", incidentAssignmentEventDestination,
                IncidentAssignmentEventBuilder::builder);
        addPayloadBuilder("UpdateResponder", "UpdateResponderCommand", updateResponderCommandDestination,
                UpdateResponderCommandBuilder::builder);
    }

    void addPayloadBuilder(String payloadType, String messageType, String destination,
            BiFunction<Pair<String, String>, Map<String, Object>, CloudEvent> builder) {
        payloadBuilders.put(payloadType, new ImmutablePair<>(new ImmutablePair<>(messageType, destination), builder));
    }
}
