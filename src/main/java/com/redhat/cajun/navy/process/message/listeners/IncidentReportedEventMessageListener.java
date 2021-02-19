package com.redhat.cajun.navy.process.message.listeners;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.cajun.navy.process.message.model.IncidentReportedEvent;
import com.redhat.cajun.navy.rules.model.Incident;
import io.cloudevents.CloudEvent;
import org.jbpm.services.api.ProcessService;
import org.kie.internal.KieInternalServices;
import org.kie.internal.process.CorrelationKey;
import org.kie.internal.process.CorrelationKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
public class IncidentReportedEventMessageListener {

    private final static Logger log = LoggerFactory.getLogger(IncidentReportedEventMessageListener.class);

    private static final String TYPE_INCIDENT_REPORTED_EVENT = "IncidentReportedEvent";

    private CorrelationKeyFactory correlationKeyFactory = KieInternalServices.Factory.get().newCorrelationKeyFactory();

    @Autowired
    private ProcessService processService;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Value("${incident.deployment.id}")
    private String deploymentId;

    @Value("${incident.process.id}")
    private String processId;

    @Value("${incident.process.assignment-delay}")
    private String assignmentDelay;

    @KafkaListener(topics = "${listener.destination.incident-reported-event}")
    public void processMessage(@Payload CloudEvent cloudEvent, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, Acknowledgment ack) {

        if (!accept(cloudEvent)) {
            ack.acknowledge();
            return;
        }
        log.debug("Processing 'IncidentReportedEvent' message for incident " + key + " from topic:partition " + topic + ":" + partition);
        doProcessMessage(cloudEvent, ack);
    }

    private void doProcessMessage(CloudEvent cloudEvent, Acknowledgment ack) {
        IncidentReportedEvent incidentReportedEvent;
        try {

            incidentReportedEvent = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(cloudEvent.getData().toBytes(), new TypeReference<IncidentReportedEvent>() {});

            validate(incidentReportedEvent);

            String incidentId = incidentReportedEvent.getId();

            Incident incident = new Incident();
            incident.setId(incidentId);
            incident.setLatitude(incidentReportedEvent.getLat());
            incident.setLongitude(incidentReportedEvent.getLon());
            incident.setNumPeople(incidentReportedEvent.getNumberOfPeople());
            incident.setMedicalNeeded(incidentReportedEvent.isMedicalNeeded());
            incident.setReportedTime(incidentReportedEvent.getTimestamp());

            Map<String, Object> parameters = new HashMap<>();
            parameters.put("incident", incident);
            parameters.put("assignmentDelay", assignmentDelay);

            CorrelationKey correlationKey = correlationKeyFactory.newCorrelationKey(incidentId);

            TransactionTemplate template = new TransactionTemplate(transactionManager);
            template.execute((TransactionStatus s) -> {
                Long pi = processService.startProcess(deploymentId, processId, correlationKey, parameters);
                log.debug("Started incident process for incident " + incidentId + ". ProcessInstanceId = " + pi);
                return null;
            });
        } catch (Exception e) {
            log.error("Error processing CloudEvent " + cloudEvent, e);
        }
        ack.acknowledge();
    }

    private boolean accept(CloudEvent cloudEvent) {
        if (cloudEvent == null) {
            log.warn("Message is not a CloudEvent. Message is ignored");
            //TODO: exception logging
            return false;
        }
        String messageType = cloudEvent.getType();
        if (!(TYPE_INCIDENT_REPORTED_EVENT.equalsIgnoreCase(messageType))) {
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

    private void validate(IncidentReportedEvent ire) {
        if (ire.getId() == null || ire.getId().isEmpty() || ire.getLat() == null
                || ire.getLon() == null || ire.getNumberOfPeople() == 0) {
            throw new IllegalStateException("Missing fields in IncidentReportedEvent: " + ire.toString());
        }
    }
}
