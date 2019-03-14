package com.redhat.cajun.navy.process.message.model;

import java.util.UUID;

public class Message<T> {

    private String id;

    private String messageType;

    private String invokingService;

    private long timestamp;

    private T body;

    public String getMessageType() {
        return messageType;
    }

    public String getId() {
        return id;
    }

    public String getInvokingService() {
        return invokingService;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public T getBody() {
        return body;
    }

    public static class Builder<T> {

        private final String messageType;
        private final String invokingService;
        private final T body;

        private String id = UUID.randomUUID().toString();
        private long timestamp = System.currentTimeMillis();

        public Builder(String messageType, String invokingService, T body) {

            this.messageType = messageType;
            this.invokingService = invokingService;
            this.body = body;
        }

        public Builder<T> id(String id) {
            this.id = id;
            return this;
        }

        public Builder<T> timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Message<T> build() {
            Message<T> msg = new Message<T>();
            msg.messageType = this.messageType;
            msg.invokingService = this.invokingService;
            msg.body = this.body;
            msg.id = this.id;
            msg.timestamp = this.timestamp;
            return msg;
        }
    }
}
