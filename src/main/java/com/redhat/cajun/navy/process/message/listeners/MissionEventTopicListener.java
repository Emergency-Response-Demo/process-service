package com.redhat.cajun.navy.process.message.listeners;

import java.util.Arrays;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.redhat.cajun.navy.process.message.model.Message;
import com.redhat.cajun.navy.process.message.model.MissionStartedEvent;
import com.redhat.cajun.navy.process.message.model.VictimDeliveredEvent;
import com.redhat.cajun.navy.process.message.model.VictimPickedUpEvent;
import org.jbpm.services.api.ProcessService;
import org.jbpm.services.api.query.QueryService;
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

    private CorrelationKeyFactory correlationKeyFactory = KieInternalServices.Factory.get().newCorrelationKeyFactory();

    @Autowired
    private ProcessService processService;

    @Autowired
    private QueryService queryService;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @KafkaListener(topics = "${listener.destination.mission-event}")
    public void processMessage(@Payload String messageAsJson,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, Acknowledgment ack) {

        messageType(messageAsJson, ack).ifPresent(s -> {
            switch (s) {
                case TYPE_MISSION_STARTED_EVENT:
                    processMissionStartedEvent(messageAsJson, topic, partition, ack);
                    break;
                case TYPE_MISSION_PICKEDUP_EVENT:
                    processVictimPickedUpEvent(messageAsJson, topic, partition, ack);
                    break;
                case TYPE_MISSION_COMPLETED_EVENT:
                    processVictimDeliveredEvent(messageAsJson, topic, partition, ack);
                    break;
            }
        });
    }

    private void processMissionStartedEvent(String messageAsJson, String topic, int partition, Acknowledgment ack) {
        Message<MissionStartedEvent> message;
        try {
            message = new ObjectMapper().readValue(messageAsJson, new TypeReference<Message<MissionStartedEvent>>() {});
            String incidentId = message.getBody().getIncidentId();
            log.debug("Processing '" + TYPE_MISSION_STARTED_EVENT + "' message for incident '" + incidentId + "' from topic:partition " + topic + ":" + partition);
            signalProcess(incidentId, SIGNAL_MISSION_STARTED);
            ack.acknowledge();
       } catch (Exception e) {
            log.error("Error processing msg " + messageAsJson, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void processVictimPickedUpEvent(String messageAsJson, String topic, int partition, Acknowledgment ack) {
        Message<VictimPickedUpEvent> message;
        try {
            message = new ObjectMapper().readValue(messageAsJson, new TypeReference<Message<VictimPickedUpEvent>>() {});
            String incidentId = message.getBody().getIncidentId();
            log.debug("Processing '" + TYPE_MISSION_PICKEDUP_EVENT + "' message for incident '" + incidentId + "' from topic:partition " + topic + ":" + partition);
            signalProcess(incidentId, SIGNAL_VICTIM_PICKEDUP);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing msg " + messageAsJson, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void processVictimDeliveredEvent(String messageAsJson, String topic, int partition, Acknowledgment ack) {
        Message<VictimDeliveredEvent> message;
        try {
            message = new ObjectMapper().readValue(messageAsJson, new TypeReference<Message<VictimDeliveredEvent>>() {});
            String incidentId = message.getBody().getIncidentId();
            log.debug("Processing '" + TYPE_MISSION_COMPLETED_EVENT + "' message for incident '" + incidentId + "' from topic:partition " + topic + ":" + partition);
            signalProcess(incidentId, SIGNAL_VICTIM_DELIVERED);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing msg " + messageAsJson, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void signalProcess(String incidentId, String signal) throws Exception {
        if (incidentId == null || incidentId.isEmpty()) {
            log.warn("Message contains no value for incidentId. Message cannot be processed!");
            return;
        }
        final IntegerHolder holder = new IntegerHolder(5);
        while (holder.counting()) {
            TransactionTemplate template = new TransactionTemplate(transactionManager);
            template.execute((TransactionStatus s) -> {
                // check if process is waiting on signal
                if (!SignalsByCorrelationKeyHelper.waitingForSignal(queryService, incidentId, signal)) {
                    log.warn("Try " + holder.getValue() + " - Process instance with correlationKey '" + incidentId + "' is not waiting for signal '" + signal + "'.");
                    holder.add();
                    return null;
                }
                holder.reset();
                return null;
            });
            if (holder.limit()) {
                log.warn("Process instance with correlationKey '" + incidentId + "' is not waiting for signal '" + signal + "'. Process instance is not signaled.");
            } else if (holder.counting()) {
                log.info("Sleeping for 300 ms");
                Thread.sleep(300);
            }
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

    private Optional<String> messageType(String messageAsJson, Acknowledgment ack) {
        try {
            String messageType = JsonPath.read(messageAsJson, "$.messageType");
            if (Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType)) {
                return Optional.of(messageType);
            }
            log.debug("Message with type '" + messageType + "' is ignored");
        } catch (Exception e) {
            log.warn("Unexpected message without 'messageType' field.");
        }
        ack.acknowledge();
        return Optional.empty();
    }

    public static class IntegerHolder {

        private int value;

        private int limit;

        public IntegerHolder(int limit) {
            value = 1;
            this.limit = limit;
        }

        public void add() {
            value++;
        }

        public int getValue() {
            return value;
        }

        public void reset() {
            value = 0;
        }

        public boolean limit() {
            return value > limit;
        }

        public boolean counting() {
            return value > 0 && value <= limit;
        }

        public boolean done() {
            return value == 0;
        }

    }
}
