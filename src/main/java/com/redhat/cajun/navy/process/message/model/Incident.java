package com.redhat.cajun.navy.process.message.model;

public class Incident {

    public enum IncidentStatus {REPORTED, ASSIGNED, PICKEDUP, CANCELLED, RESCUED}

    private String id;

    private String status;

    public String getId() {
        return id;
    }

    public String getStatus() {
        return status;
    }

    public static class Builder {

        private final Incident incident;

        public Builder(String id, IncidentStatus incidentStatus) {
            incident = new Incident();
            incident.id = id;
            incident.status = incidentStatus.name();
        }

        public Incident build() {
            return incident;
        }

    }

}
