package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.CreateMissionCommand;
import com.redhat.cajun.navy.rules.model.Mission;

public class CreateMissionCommandBuilder {

    public static KafkaMessageSenderWorkItemHandler.Pair<String, CreateMissionCommand> builder(Map<String, Object> parameters) {

        Object payload = parameters.get("payload");
        if (!(payload instanceof Mission)) {
            throw new IllegalStateException("Parameter 'payload' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Mission");
        }
        Mission mission = (Mission)payload;
        return new KafkaMessageSenderWorkItemHandler.Pair<>(mission.getIncidentId(), new CreateMissionCommand.Builder()
                .incidentId(mission.getIncidentId())
                .incidentLat(mission.getIncidentLat().toString())
                .incidentLong(mission.getIncidentLong().toString())
                .responderId(mission.getResponderId())
                .responderStartLat(mission.getResponderStartLat().toString())
                .responderStartLong(mission.getResponderStartLong().toString())
                .destinationLat(mission.getDestinationLat().toString())
                .destinationLong(mission.getDestinationLong().toString())
                .build());
    }

}
