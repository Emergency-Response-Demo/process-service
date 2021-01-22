package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.CloudEventBuilder;
import com.redhat.cajun.navy.process.message.model.CreateMissionCommand;
import com.redhat.cajun.navy.rules.model.Mission;
import io.cloudevents.CloudEvent;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class CreateMissionCommandBuilder {

    public static Pair<String, CloudEvent> builder(String messageType, Map<String, Object> parameters) {

        Object payload = parameters.get("Payload");
        if (!(payload instanceof Mission)) {
            throw new IllegalStateException("Parameter 'payload' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Mission");
        }
        Object processId = parameters.get("processId");
        if (processId == null || processId.equals("")) {
            throw new IllegalStateException("Parameter 'processId' cannot be null and must be of type String");
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
                .processId((String)processId)
                .build();
        CloudEvent cloudEvent = new CloudEventBuilder<CreateMissionCommand>()
                .withType(messageType)
                .withData(command)
                .build();
        return new ImmutablePair<>(mission.getIncidentId(), cloudEvent);
    }

}
