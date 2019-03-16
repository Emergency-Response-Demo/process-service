package com.redhat.cajun.navy.process.wih;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.PostConstruct;

import com.redhat.cajun.navy.process.message.model.Message;
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

    private Map<String, Pair<String, Function<Map<String, Object>, ?>>> payloadBuilders = new HashMap<>();

    public KafkaMessageSenderWorkItemHandler() {

    }

    @Override
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        Map<String, Object> parameters = workItem.getParameters();
        Object messageType = parameters.get("MessageType");
        if (!(messageType instanceof String)) {
            throw new IllegalStateException("Parameter 'messageType' cannot be null and must be of type String");
        }
        Pair<String, Function<Map<String, Object>, ?>> destinationAndBuilderPair = payloadBuilders.get(messageType);
        if (destinationAndBuilderPair == null) {
            throw new IllegalStateException("No builder found for payload'" + messageType + "'");
        }
        Pair<String, ?> keyAndPayloadPair = (Pair<String, ?>) destinationAndBuilderPair.right().apply(parameters);
        Message<Object> message  = new Message.Builder<Object>((String)messageType, "ProcessService",
                keyAndPayloadPair.right()).build();
        send(keyAndPayloadPair.left(), message, destinationAndBuilderPair.left());
        manager.completeWorkItem(workItem.getId(), Collections.emptyMap());
    }

    private void send(String key, Message<?> msg, String destination) {
        ListenableFuture<SendResult<String, Message<?>>> future = kafkaTemplate.send(destination, key, msg);
        future.addCallback(
                result -> log.debug("Sent '" + msg.getMessageType() + "' message for incident " + key),
                ex -> log.error("Error sending '" + msg.getMessageType() + "' message for incident " + key, ex));
    }

    @Override
    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {

    }

    @PostConstruct
    public void init() {
        addPayloadBuilder("CreateMissionCommand", createMissionCommandDestination, CreateMissionCommandBuilder::builder);
    }

    void addPayloadBuilder(String payloadType, String destination, Function<Map<String, Object>, ? extends Object> builder) {
        payloadBuilders.put(payloadType, new Pair<>(destination, builder));
    }

    public static class Pair<L, R> {

        private final L left;

        private final R right;

        public Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }

        public L left() {
            return left;
        }

        public R right() {
            return right;
        }

    }
}
