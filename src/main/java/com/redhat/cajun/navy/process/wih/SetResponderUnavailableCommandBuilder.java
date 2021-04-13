package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.CloudEventBuilder;
import com.redhat.cajun.navy.process.message.model.Responder;
import com.redhat.cajun.navy.process.message.model.SetResponderUnavailableCommand;
import com.redhat.cajun.navy.rules.model.Mission;
import io.cloudevents.CloudEvent;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class SetResponderUnavailableCommandBuilder {

    public static Pair<String, CloudEvent> builder(String messageType, Map<String, Object> parameters) {

        Object payload = parameters.get("Payload");
        if (!(payload instanceof Mission)) {
            throw new IllegalStateException("Parameter 'payload' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Mission");
        }
        Mission mission = (Mission) payload;
        Responder responder = new Responder.Builder(mission.getResponderId()).available(false).build();
        SetResponderUnavailableCommand command = new SetResponderUnavailableCommand.Builder(responder).build();
        CloudEvent cloudEvent = new CloudEventBuilder<SetResponderUnavailableCommand>()
                .withType(messageType)
                .withData(command)
                .withExtension("incidentid", mission.getIncidentId())
                .withExtension("aggregatetype", "responder-command")
                .withExtension("aggregateid", mission.getResponderId())
                .build();

        return new ImmutablePair<>(mission.getResponderId(), cloudEvent);
    }
}
