package com.redhat.cajun.navy.process.message.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SetResponderUnavailableCommand {

    private Responder responder;

    public Responder getResponder() {
        return responder;
    }

    public static class Builder {

        private final SetResponderUnavailableCommand command;

        public Builder(Responder responder) {
            command = new SetResponderUnavailableCommand();
            command.responder = responder;
        }

        public SetResponderUnavailableCommand build() {
            return command;
        }

    }

}
