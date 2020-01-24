package com.redhat.cajun.navy.process.message.model;

public class IncidentAssignmentEvent {

    private String incidentId;

    private Boolean assignment;

    private String lat;

    private String lon;

    public String getIncidentId() {
        return incidentId;
    }

    public Boolean getAssignment() {
        return assignment;
    }

    public String getLat() {
        return lat;
    }

    public String getLon() {
        return lon;
    }

    public static class Builder {

        private final IncidentAssignmentEvent event;

        public Builder(String incidentId, Boolean assignment, String lat, String lon) {
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
