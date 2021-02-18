package com.redhat.cajun.navy.process.message.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class CreateMissionCommand {

    private String incidentId;

    private String responderId;

    private BigDecimal responderStartLat;

    private BigDecimal responderStartLong;

    private BigDecimal incidentLat;

    private BigDecimal incidentLong;

    private BigDecimal destinationLat;

    private BigDecimal destinationLong;

    private String processId;

    public String getIncidentId() {
        return incidentId;
    }

    public String getResponderId() {
        return responderId;
    }

    public BigDecimal getResponderStartLat() {
        return responderStartLat;
    }

    public BigDecimal getResponderStartLong() {
        return responderStartLong;
    }

    public BigDecimal getIncidentLat() {
        return incidentLat;
    }

    public BigDecimal getIncidentLong() {
        return incidentLong;
    }

    public BigDecimal getDestinationLat() {
        return destinationLat;
    }

    public BigDecimal getDestinationLong() {
        return destinationLong;
    }

    public String getProcessId() {
        return processId;
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

        public Builder responderStartLat(BigDecimal responderStartLat) {
            command.responderStartLat = responderStartLat;
            return this;
        }

        public Builder responderStartLong(BigDecimal responderStartLong) {
            command.responderStartLong = responderStartLong;
            return this;
        }

        public Builder incidentLat(BigDecimal incidentLat) {
            command.incidentLat = incidentLat;
            return this;
        }

        public Builder incidentLong(BigDecimal incidentLong) {
            command.incidentLong = incidentLong;
            return this;
        }

        public Builder destinationLat(BigDecimal destinationLat) {
            command.destinationLat = destinationLat;
            return this;
        }

        public Builder destinationLong(BigDecimal destinationLong) {
            command.destinationLong = destinationLong;
            return this;
        }

        public Builder processId(String processId) {
            command.processId = processId;
            return this;
        }

        public CreateMissionCommand build() {
            return command;
        }
    }
}
