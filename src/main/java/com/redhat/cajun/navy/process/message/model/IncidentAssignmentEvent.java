package com.redhat.cajun.navy.process.message.model;

import java.math.BigDecimal;

public class IncidentAssignmentEvent {

    private String incidentId;

    private Boolean assignment;

    private BigDecimal lat;

    private BigDecimal lon;

    public String getIncidentId() {
        return incidentId;
    }

    public Boolean getAssignment() {
        return assignment;
    }

    public BigDecimal getLat() {
        return lat;
    }

    public BigDecimal getLon() {
        return lon;
    }

    public static class Builder {

        private final IncidentAssignmentEvent event;

        public Builder(String incidentId, Boolean assignment, BigDecimal lat, BigDecimal lon) {
            event = new IncidentAssignmentEvent();
            event.incidentId = incidentId;
            event.assignment = assignment;
            event.lat = lat;
            event.lon = lon;
        }

        public IncidentAssignmentEvent build() {
            return event;
        }


    }
}
