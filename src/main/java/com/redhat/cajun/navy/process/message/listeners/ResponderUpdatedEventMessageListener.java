package com.redhat.cajun.navy.process.message.listeners;

import java.io.IOException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.cajun.navy.process.message.model.ResponderUpdatedEvent;
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
public class ResponderUpdatedEventMessageListener {

    private static final Logger log = LoggerFactory.getLogger(ResponderUpdatedEventMessageListener.class);

    private final static String TYPE_RESPONDER_UPDATED_EVENT = "ResponderUpdatedEvent";

    private static final String SIGNAL_RESPONDER_AVAILABLE = "ResponderAvailable";

    @Autowired
    private ProcessService processService;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private final CorrelationKeyFactory correlationKeyFactory = KieInternalServices.Factory.get().newCorrelationKeyFactory();

    @KafkaListener(topics = "${listener.destination.responder-updated-event}")
    public void processMessage(@Payload CloudEvent cloudEvent, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, Acknowledgment ack) {

        if (!accept(cloudEvent)) {
            ack.acknowledge();
            return;
        }

        log.debug("Processing '" + TYPE_RESPONDER_UPDATED_EVENT + "' CloudEvent for responder '" + key + "' from topic:partition '" + topic + ":" + partition + "'");

        String incidentId = (String) cloudEvent.getExtension("incidentid");
        if (incidentId == null || incidentId.isEmpty()) {
            log.warn("CloudEvent contains no extension value for incidentId. Message cannot be processed!");
            ack.acknowledge();
            return;
        }

        try {
            ResponderUpdatedEvent message = new ObjectMapper().readValue(cloudEvent.getData().toBytes(), new TypeReference<ResponderUpdatedEvent>() {});
        } catch (IOException e) {
            log.error("CloudEvent data cannot be unmarshalled to ResponderUpdatedEvent object. Message is ignored.");
            ack.acknowledge();
            return;
        }

        try {

            ResponderUpdatedEvent message = new ObjectMapper().readValue(cloudEvent.getData().toBytes(), new TypeReference<ResponderUpdatedEvent>() {});

            CorrelationKey correlationKey = correlationKeyFactory.newCorrelationKey(incidentId);

            Boolean available = "success".equals(message.getStatus());

            log.debug("Signaling process with correlationkey '" + correlationKey + ". Responder '" + key + "', available '" + available + "'." );

            TransactionTemplate template = new TransactionTemplate(transactionManager);
            template.execute((TransactionStatus s) -> {
                ProcessInstance instance = processService.getProcessInstance(correlationKey);
                processService.signalProcessInstance(instance.getId(), SIGNAL_RESPONDER_AVAILABLE, available);
                return null;
            });

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing CloudEvent " + cloudEvent, e);
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    private boolean accept(CloudEvent cloudEvent) {
        if (cloudEvent == null) {
            log.warn("Message is not a CloudEvent. Message is ignored");
            //TODO: exception logging
            return false;
        }
        String messageType = cloudEvent.getType();
        if (!(TYPE_RESPONDER_UPDATED_EVENT.equalsIgnoreCase(messageType) )) {
            log.debug("Message with type '" + messageType + "' is ignored");
            return false;
        }
        String contentType = cloudEvent.getDataContentType();
        if (contentType == null || !(contentType.equalsIgnoreCase("application/json"))) {
            log.warn("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
            return false;
        }
        if (cloudEvent.getData() == null) {
            log.warn("CloudEvent contains no data. Message is ignored");
            return false;
        }
        return true;
    }
}
