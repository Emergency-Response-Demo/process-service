package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.IncidentAssignmentEvent;
import com.redhat.cajun.navy.process.message.model.Message;
import com.redhat.cajun.navy.rules.model.Mission;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class IncidentAssignmentEventBuilder {

    public static Pair<String, Message<?>> builder(String messageType, Map<String, Object> parameters) {

        Object payload = parameters.get("Payload");
        if (!(payload instanceof Mission)) {
            throw new IllegalStateException("Parameter 'payload' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Mission");
        }
        Mission mission = (Mission) payload;
        IncidentAssignmentEvent event = new IncidentAssignmentEvent.Builder(mission.getIncidentId(), "ASSIGNED".equals(mission.getStatus().name()), mission.getIncidentLat().toString(), mission.getIncidentLong().toString()).build();
        return new ImmutablePair<>(mission.getIncidentId(), new Message.Builder<>(messageType, "IncidentProcessService", event).build());
    }

}
