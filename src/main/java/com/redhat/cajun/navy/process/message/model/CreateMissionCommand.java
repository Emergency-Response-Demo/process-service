package com.redhat.cajun.navy.process.message.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class CreateMissionCommand {

    private String incidentId;

    private String responderId;

    private String responderStartLat;

    private String responderStartLong;

    private String incidentLat;

    private String incidentLong;

    private String destinationLat;

    private String destinationLong;

    public String getIncidentId() {
        return incidentId;
    }

    public String getResponderId() {
        return responderId;
    }

    public String getResponderStartLat() {
        return responderStartLat;
    }

    public String getResponderStartLong() {
        return responderStartLong;
    }

    public String getIncidentLat() {
        return incidentLat;
    }

    public String getIncidentLong() {
        return incidentLong;
    }

    public String getDestinationLat() {
        return destinationLat;
    }

    public String getDestinationLong() {
        return destinationLong;
    }

    public static class Builder {

        private CreateMissionCommand command;

        public Builder() {
            command = new CreateMissionCommand();
        }

        public Builder incidentId(String incidentId) {
            command.incidentId = incidentId;
            return this;
        }

        public Builder responderId(String responderId) {
            command.responderId = responderId;
            return this;
        }

        public Builder responderStartLat(String responderStartLat) {
            command.responderStartLat = responderStartLat;
            return this;
        }

        public Builder responderStartLong(String responderStartLong) {
            command.responderStartLong = responderStartLong;
            return this;
        }

        public Builder incidentLat(String incidentLat) {
            command.incidentLat = incidentLat;
            return this;
        }

        public Builder incidentLong(String incidentLong) {
            command.incidentLong = incidentLong;
            return this;
        }

        public Builder destinationLat(String destinationLat) {
            command.destinationLat = destinationLat;
            return this;
        }

        public Builder destinationLong(String destinationLong) {
            command.destinationLong = destinationLong;
            return this;
        }

        public CreateMissionCommand build() {
            return command;
        }
    }
}
