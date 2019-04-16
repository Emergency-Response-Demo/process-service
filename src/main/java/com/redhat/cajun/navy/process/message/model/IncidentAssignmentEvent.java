package com.redhat.cajun.navy.process.message.model;

public class IncidentAssignmentEvent {

    private String incidentId;

    private Boolean assignment;

    public String getIncidentId() {
        return incidentId;
    }

    public Boolean getAssignment() {
        return assignment;
    }

    public static class Builder {

        private final IncidentAssignmentEvent event;

        public Builder(String incidentId, Boolean assignment) {
            event = new IncidentAssignmentEvent();
            event.incidentId = incidentId;
            event.assignment = assignment;
        }

        public IncidentAssignmentEvent build() {
            return event;
        }


    }
}
