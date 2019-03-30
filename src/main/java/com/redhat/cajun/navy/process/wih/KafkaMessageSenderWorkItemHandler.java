package com.redhat.cajun.navy.process.wih;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.PostConstruct;

import com.redhat.cajun.navy.process.message.model.Message;
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
    private KafkaTemplate<String, Message<?>> kafkaTemplate;

    @Value("${sender.destination.create-mission-command}")
    private String createMissionCommandDestination;

    @Value("${sender.destination.update-responder-command}")
    private String updateResponderCommandDestination;

    private Map<String, Triple<String, String, Function<Map<String, Object>, ?>>> payloadBuilders = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        Map<String, Object> parameters = workItem.getParameters();
        Object messageType = parameters.get("MessageType");
        if (!(messageType instanceof String)) {
            throw new IllegalStateException("Parameter 'messageType' cannot be null and must be of type String");
        }
        Triple<String, String, Function<Map<String, Object>, ?>> messagetypeDestinationBuilderTuple = payloadBuilders.get(messageType);
        if (messagetypeDestinationBuilderTuple == null) {
            throw new IllegalStateException("No builder found for payload'" + messageType + "'");
        }
        Pair<String, ?> keyAndPayloadPair = (Pair<String, ?>) messagetypeDestinationBuilderTuple.getRight().apply(parameters);
        Message<Object> message  = new Message.Builder<Object>(messagetypeDestinationBuilderTuple.getLeft(), "ProcessService",
                keyAndPayloadPair.getRight()).build();
        send(messagetypeDestinationBuilderTuple.getMiddle(), keyAndPayloadPair.getLeft(), message);
        manager.completeWorkItem(workItem.getId(), Collections.emptyMap());
    }

    private void send(String destination, String key, Message<?> msg) {
        ListenableFuture<SendResult<String, Message<?>>> future = kafkaTemplate.send(destination, key, msg);
        future.addCallback(
                result -> log.debug("Sent '" + msg.getMessageType() + "' message witk key " + key),
                ex -> log.error("Error sending '" + msg.getMessageType() + "' message with key " + key, ex));
    }

    @Override
    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {

    }

    @PostConstruct
    public void init() {
        addPayloadBuilder("CreateMissionCommand", "CreateMissionCommand", createMissionCommandDestination, CreateMissionCommandBuilder::builder);
        addPayloadBuilder("SetResponderUnavailableCommand", "UpdateResponderCommand", updateResponderCommandDestination, SetResponderUnavailableCommandBuilder::builder);
    }

    void addPayloadBuilder(String payloadType, String messageType, String destination, Function<Map<String, Object>, ?> builder) {
        payloadBuilders.put(payloadType, new ImmutableTriple<>(messageType, destination, builder));
    }
}
