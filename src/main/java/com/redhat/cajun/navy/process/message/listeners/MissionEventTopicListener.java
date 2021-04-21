package com.redhat.cajun.navy.process.message.listeners;

import java.util.Arrays;
import java.util.Objects;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.cajun.navy.process.message.model.MissionCompletedEvent;
import com.redhat.cajun.navy.process.message.model.MissionPickedUpEvent;
import com.redhat.cajun.navy.process.message.model.MissionStartedEvent;
import io.cloudevents.CloudEvent;
import org.jbpm.services.api.ProcessService;
import org.kie.api.runtime.process.ProcessInstance;
import org.kie.internal.KieInternalServices;
import org.kie.internal.process.CorrelationKey;
import org.kie.internal.process.CorrelationKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class MissionEventTopicListener {

    private static final Logger log = LoggerFactory.getLogger(MissionEventTopicListener.class);

    private static final String TYPE_MISSION_STARTED_EVENT = "MissionStartedEvent";
    private static final String TYPE_MISSION_PICKEDUP_EVENT = "MissionPickedUpEvent";
    private static final String TYPE_MISSION_COMPLETED_EVENT = "MissionCompletedEvent";
    private static final String[] ACCEPTED_MESSAGE_TYPES = {TYPE_MISSION_STARTED_EVENT, TYPE_MISSION_PICKEDUP_EVENT, TYPE_MISSION_COMPLETED_EVENT};

    private static final String SIGNAL_MISSION_STARTED = "MissionStarted";
    private static final String SIGNAL_VICTIM_PICKEDUP = "VictimPickedUp";
    private static final String SIGNAL_VICTIM_DELIVERED = "VictimDelivered";

    private final CorrelationKeyFactory correlationKeyFactory = KieInternalServices.Factory.get().newCorrelationKeyFactory();

    @Autowired
    private ProcessService processService;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @KafkaListener(topics = "${listener.destination.mission-event}")
    public void processMessage(@Payload CloudEvent cloudEvent,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, Acknowledgment ack) {

        if (!accept(cloudEvent)) {
            ack.acknowledge();
            return;
        }
        String messageType = cloudEvent.getType();
        switch (messageType) {
            case TYPE_MISSION_STARTED_EVENT:
                processMissionStartedEvent(cloudEvent, topic, partition, ack);
                break;
            case TYPE_MISSION_PICKEDUP_EVENT:
                processMissionPickedUpEvent(cloudEvent, topic, partition, ack);
                break;
            case TYPE_MISSION_COMPLETED_EVENT:
                processMissionCompletedEvent(cloudEvent, topic, partition, ack);
                break;
        }
    }

    private void processMissionStartedEvent(CloudEvent cloudEvent, String topic, int partition, Acknowledgment ack) {
        MissionStartedEvent missionStartedEvent;
        try {
            missionStartedEvent = new ObjectMapper().readValue(Objects.requireNonNull(cloudEvent.getData()).toBytes(), new TypeReference<MissionStartedEvent>() {});
        } catch (Exception e) {
            log.error("Error deserializing MissionStartedEvent ", e);
            ack.acknowledge();
            return;
        }
        try {
            String incidentId = missionStartedEvent.getIncidentId();
            log.debug("Processing '" + TYPE_MISSION_STARTED_EVENT + "' message for incident '" + incidentId + "' from topic:partition " + topic + ":" + partition);
            signalProcess(incidentId, SIGNAL_MISSION_STARTED);
        } catch (Exception e) {
            log.error("Error processing CloudEvent " + cloudEvent, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
        ack.acknowledge();
    }

    private void processMissionPickedUpEvent(CloudEvent cloudEvent, String topic, int partition, Acknowledgment ack) {
        MissionPickedUpEvent missionPickedUpEvent;
        try {
            missionPickedUpEvent = new ObjectMapper().readValue(Objects.requireNonNull(cloudEvent.getData()).toBytes(), new TypeReference<MissionPickedUpEvent>() {});
        } catch (Exception e) {
            log.error("Error deserializing MissionPickedUpEvent ", e);
            ack.acknowledge();
            return;
        }
        try {
            String incidentId = missionPickedUpEvent.getIncidentId();
            log.debug("Processing '" + TYPE_MISSION_PICKEDUP_EVENT + "' message for incident '" + incidentId + "' from topic:partition " + topic + ":" + partition);
            signalProcess(incidentId, SIGNAL_VICTIM_PICKEDUP);
        } catch (Exception e) {
            log.error("Error processing CloudEvent " + cloudEvent, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
        ack.acknowledge();
    }

    private void processMissionCompletedEvent(CloudEvent cloudEvent, String topic, int partition, Acknowledgment ack) {
        MissionCompletedEvent message;
        try {
            message = new ObjectMapper().readValue(Objects.requireNonNull(cloudEvent.getData()).toBytes(), new TypeReference<MissionCompletedEvent>() {});
        } catch (Exception e) {
            log.error("Error deserializing MissionDeliveredEvent ", e);
            ack.acknowledge();
            return;
        }
        try {
            String incidentId = message.getIncidentId();
            log.debug("Processing '" + TYPE_MISSION_COMPLETED_EVENT + "' message for incident '" + incidentId + "' from topic:partition " + topic + ":" + partition);
            signalProcess(incidentId, SIGNAL_VICTIM_DELIVERED);
        } catch (Exception e) {
            log.error("Error processing CloudEvent " + cloudEvent, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
        ack.acknowledge();
    }

    private void signalProcess(String incidentId, String signal) {
        if (incidentId == null || incidentId.isEmpty()) {
            log.warn("Message contains no value for incidentId. Message cannot be processed!");
            return;
        }

        CorrelationKey correlationKey = correlationKeyFactory.newCorrelationKey(incidentId);
        new TransactionTemplate(transactionManager).execute((TransactionStatus s) -> {
            ProcessInstance processInstance = processService.getProcessInstance(correlationKey);
            if (processInstance == null) {
                log.warn("Process instance with correlationKey '" + incidentId + "' not found.");
                return null;
            }
            processService.signalProcessInstance(processInstance.getId(), signal, null);
            return null;
        });
    }

    private boolean accept(CloudEvent cloudEvent) {
        if (cloudEvent == null) {
            log.warn("Message is not a CloudEvent. Message is ignored");
            //TODO: exception logging
            return false;
        }
        String messageType = cloudEvent.getType();
        if (!(Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType))) {
            log.debug("Message with type '" + messageType + "' is ignored");
            return false;
        }
        String contentType = cloudEvent.getDataContentType();
        if (contentType == null || !(contentType.equalsIgnoreCase("application/json"))) {
            log.warn("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
            return false;
        }
        return true;
    }

}
