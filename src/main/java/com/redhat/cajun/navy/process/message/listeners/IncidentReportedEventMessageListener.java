package com.redhat.cajun.navy.process.message.listeners;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.redhat.cajun.navy.process.message.model.DestinationLocations;
import com.redhat.cajun.navy.process.message.model.IncidentReportedEvent;
import com.redhat.cajun.navy.process.message.model.Message;
import com.redhat.cajun.navy.rules.model.Destination;
import com.redhat.cajun.navy.rules.model.Destinations;
import com.redhat.cajun.navy.rules.model.Incident;
import org.jbpm.services.api.ProcessService;
import org.kie.internal.KieInternalServices;
import org.kie.internal.process.CorrelationKey;
import org.kie.internal.process.CorrelationKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
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

    @Autowired
    private DestinationLocations destinationLocations;

    @KafkaListener(topics = "${listener.destination.incident-reported-event}")
    public void processMessage(@Payload String messageAsJson, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {

        if (!accept(messageAsJson)) {
            return;
        }
        log.debug("Processing 'IncidentReportedEvent' message for incident " + key + " from topic:partition " + topic + ":" + partition);
        doProcessMessage(messageAsJson, key);
    }

    private void doProcessMessage(String messageAsJson, String key) {
        Message<IncidentReportedEvent> message;
        try {

            message = new ObjectMapper().readValue(messageAsJson, new TypeReference<Message<IncidentReportedEvent>>() {});

            String incidentId = message.getBody().getId();

            Incident incident = new Incident();
            incident.setId(message.getBody().getId());
            incident.setLatitude(message.getBody().getLat());
            incident.setLongitude(message.getBody().getLon());
            incident.setNumPeople(message.getBody().getNumberOfPeople());
            incident.setMedicalNeeded(message.getBody().isMedicalNeeded());
            incident.setReportedTime(message.getBody().getTimestamp());

            List<Destination> destinationList = destinationLocations.getLocations().stream().map(location -> {
                Destination destination = new Destination();
                destination.setName(location.getName());
                destination.setLatitude(new BigDecimal(location.getLatitude()));
                destination.setLongitude(new BigDecimal(location.getLongitude()));
                return destination;
            }).collect(Collectors.toList());

            Destinations destinations = new Destinations(destinationList);

            Map<String, Object> parameters = new HashMap<>();
            parameters.put("incident", incident);
            parameters.put("destinations", destinations);

            CorrelationKey correlationKey = correlationKeyFactory.newCorrelationKey(incidentId);

            TransactionTemplate template = new TransactionTemplate(transactionManager);
            template.execute((TransactionStatus s) -> {
                Long pi = processService.startProcess(deploymentId, processId, correlationKey, parameters);
                log.debug("Started incident process for incident " + incidentId + ". ProcessInstanceId = " + pi);
                return null;
            });
        } catch (Exception e) {
            log.error("Error processing msg " + messageAsJson, e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private boolean accept(String messageAsJson) {
        try {
            String messageType = JsonPath.read(messageAsJson, "$.messageType");
            if (TYPE_INCIDENT_REPORTED_EVENT.equalsIgnoreCase(messageType) ) {
                return true;
            } else {
                log.debug("Message with type '" + messageType + "' is ignored");
            }
        } catch (Exception e) {
            log.warn("Unexpected message without 'messageType' field.");
        }
        return false;
    }
}
