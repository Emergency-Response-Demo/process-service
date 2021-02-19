package com.redhat.cajun.navy.process.message.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MissionPickedUpEvent {

    private String missionId;

    private String incidentId;

    public String getMissionId() {
        return missionId;
    }

    public String getIncidentId() {
        return incidentId;
    }

}
