package com.redhat.cajun.navy.process.message.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class UpdateResponderCommand {

    private Responder responder;

    public Responder getResponder() {
        return responder;
    }

    public static class Builder {

        private final UpdateResponderCommand command;

        public Builder(Responder responder) {
            command = new UpdateResponderCommand();
            command.responder = responder;
        }

        public UpdateResponderCommand build() {
            return command;
        }

    }

}
