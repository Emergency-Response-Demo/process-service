package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.CloudEventBuilder;
import com.redhat.cajun.navy.process.message.model.Incident;
import com.redhat.cajun.navy.process.message.model.UpdateIncidentCommand;
import io.cloudevents.CloudEvent;
import org.apache.commons.lang3.tuple.Pair;

public class UpdateIncidentCommandBuilder {

    public static CloudEvent builder(Pair<String, String> messageTypeAndDestination, Map<String, Object> parameters) {

        Object payload = parameters.get("Payload");
        if (!(payload instanceof com.redhat.cajun.navy.rules.model.Incident)) {
            throw new IllegalStateException("Parameter 'payload' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Incident");
        }
        com.redhat.cajun.navy.rules.model.Incident incident = (com.redhat.cajun.navy.rules.model.Incident) payload;
        UpdateIncidentCommand command = new UpdateIncidentCommand.Builder(
                new Incident.Builder(incident.getId(), status(incident.getStatus())).build())
                .build();
        return new CloudEventBuilder<UpdateIncidentCommand>()
                .withType(messageTypeAndDestination.getLeft())
                .withData(command)
                .withExtension("aggregatetype", messageTypeAndDestination.getRight())
                .withExtension("aggregateid", incident.getId())
                .build();
    }

    private static com.redhat.cajun.navy.process.message.model.Incident.IncidentStatus status(String status) {
        if (status == null || status.isEmpty()) {
            throw new IllegalStateException("Parameter 'payload' of type com.redhat.cajun.navy.rules.model.Incident cannot have status with value null or empty");
        }
        switch (status.toLowerCase()) {
            case "assigned":
                return com.redhat.cajun.navy.process.message.model.Incident.IncidentStatus.ASSIGNED;
            case "pickedup":
                return com.redhat.cajun.navy.process.message.model.Incident.IncidentStatus.PICKEDUP;
            case "delivered":
                return com.redhat.cajun.navy.process.message.model.Incident.IncidentStatus.RESCUED;
            default:
                throw new IllegalStateException("Parameter 'payload' of type com.redhat.cajun.navy.rules.model.Incident : unrecognized status value '" + status + "'");

        }
    }
}
