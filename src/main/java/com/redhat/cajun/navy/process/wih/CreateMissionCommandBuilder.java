package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.CreateMissionCommand;
import com.redhat.cajun.navy.process.message.model.Message;
import com.redhat.cajun.navy.rules.model.Mission;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class CreateMissionCommandBuilder {

    public static Pair<String, Message<?>> builder(String messageType, Map<String, Object> parameters) {

        Object payload = parameters.get("Payload");
        if (!(payload instanceof Mission)) {
            throw new IllegalStateException("Parameter 'payload' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Mission");
        }
        Mission mission = (Mission)payload;
        CreateMissionCommand command = new CreateMissionCommand.Builder()
                .incidentId(mission.getIncidentId())
                .incidentLat(mission.getIncidentLat().toString())
                .incidentLong(mission.getIncidentLong().toString())
                .responderId(mission.getResponderId())
                .responderStartLat(mission.getResponderStartLat().toString())
                .responderStartLong(mission.getResponderStartLong().toString())
                .destinationLat(mission.getDestinationLat().toString())
                .destinationLong(mission.getDestinationLong().toString())
                .build();
        return new ImmutablePair<>(mission.getIncidentId(), new Message.Builder<>(messageType, "IncidentProcessService", command).build());
    }

}
