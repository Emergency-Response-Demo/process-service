package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.CloudEventBuilder;
import com.redhat.cajun.navy.process.message.model.CreateMissionCommand;
import com.redhat.cajun.navy.rules.model.Mission;
import io.cloudevents.CloudEvent;
import org.apache.commons.lang3.tuple.Pair;

public class CreateMissionCommandBuilder {

    public static CloudEvent builder(Pair<String, String> messageTypeAndDestination, Map<String, Object> parameters) {

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
                .incidentLat(mission.getIncidentLat())
                .incidentLong(mission.getIncidentLong())
                .responderId(mission.getResponderId())
                .responderStartLat(mission.getResponderStartLat())
                .responderStartLong(mission.getResponderStartLong())
                .destinationLat(mission.getDestinationLat())
                .destinationLong(mission.getDestinationLong())
                .processId((String)processId)
                .build();
        return new CloudEventBuilder<CreateMissionCommand>()
                .withType(messageTypeAndDestination.getLeft())
                .withData(command)
                .withExtension("aggregatetype", messageTypeAndDestination.getRight())
                .withExtension("aggregateid", mission.getIncidentId())
                .build();
    }

}
