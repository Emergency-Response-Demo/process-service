package com.redhat.cajun.navy.process.wih;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import javax.annotation.PostConstruct;

import com.redhat.cajun.navy.process.outbox.OutboxEventEmitter;
import io.cloudevents.CloudEvent;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.api.runtime.process.WorkItemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component("SendMessage")
public class KafkaMessageSenderWorkItemHandler implements WorkItemHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageSenderWorkItemHandler.class);

    @Autowired
    private KafkaTemplate<String, CloudEvent> kafkaTemplate;

    @Autowired
    private OutboxEventEmitter outboxEventEmmitter;

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

    private Map<String, Triple<String, String, BiFunction<String, Map<String, Object>, Pair<String, CloudEvent>>>> payloadBuilders = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        Map<String, Object> parameters = workItem.getParameters();
        Object messageType = parameters.get("MessageType");
        if (!(messageType instanceof String)) {
            throw new IllegalStateException("Parameter 'messageType' cannot be null and must be of type String");
        }
        Triple<String, String, BiFunction<String, Map<String, Object>, Pair<String, CloudEvent>>> messagetypeDestinationBuilderTuple = payloadBuilders
                .get(messageType);
        if (messagetypeDestinationBuilderTuple == null) {
            throw new IllegalStateException("No builder found for payload '" + messageType + "'");
        }

        parameters.put("processId", Long.toString(workItem.getProcessInstanceId()));

        Pair<String, CloudEvent> keyAndMessagePair = messagetypeDestinationBuilderTuple.getRight()
                .apply(messagetypeDestinationBuilderTuple.getLeft(), parameters);

        send(messagetypeDestinationBuilderTuple.getMiddle(), keyAndMessagePair.getLeft(), keyAndMessagePair.getRight());
        manager.completeWorkItem(workItem.getId(), Collections.emptyMap());
    }

    private void send(String destination, String key, CloudEvent cloudEvent) {
        if (cloudEvent.getType().equals("SetResponderUnavailableCommand")) {
            outboxEventEmmitter.emitCloudEvent(cloudEvent);
        } else {
            ListenableFuture<SendResult<String, CloudEvent>> future = kafkaTemplate.send(destination, key, cloudEvent);
            future.addCallback(
                    result -> log.debug(
                            "Sent '" + cloudEvent.getType() + "' CloudEvent message with key " + key + " to topic " + destination),
                    ex -> log.error("Error sending '" + cloudEvent.getType() + "' CloudEvent message with key " + key, ex));
        }
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
            BiFunction<String, Map<String, Object>, Pair<String, CloudEvent>> builder) {
        payloadBuilders.put(payloadType, new ImmutableTriple<>(messageType, destination, builder));
    }
}
