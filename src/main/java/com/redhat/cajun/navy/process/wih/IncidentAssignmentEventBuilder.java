package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.CloudEventBuilder;
import com.redhat.cajun.navy.process.message.model.IncidentAssignmentEvent;
import com.redhat.cajun.navy.rules.model.Mission;
import io.cloudevents.CloudEvent;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class IncidentAssignmentEventBuilder {

    public static Pair<String, CloudEvent> builder(String messageType, Map<String, Object> parameters) {

        Object payload = parameters.get("Payload");
        if (!(payload instanceof Mission)) {
            throw new IllegalStateException("Parameter 'payload' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Mission");
        }
        Mission mission = (Mission) payload;
        IncidentAssignmentEvent event = new IncidentAssignmentEvent.Builder(mission.getIncidentId(), "ASSIGNED".equals(mission.getStatus().name()),
                mission.getIncidentLat(), mission.getIncidentLong()).build();
        CloudEvent cloudEvent = new CloudEventBuilder<IncidentAssignmentEvent>()
                .withType(messageType)
                .withData(event)
                .build();
        return new ImmutablePair<>(mission.getIncidentId(), cloudEvent);
    }

}
