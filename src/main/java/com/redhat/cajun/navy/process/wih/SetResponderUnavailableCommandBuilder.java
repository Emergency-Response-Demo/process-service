package com.redhat.cajun.navy.process.wih;

import java.util.Map;

import com.redhat.cajun.navy.process.message.model.Responder;
import com.redhat.cajun.navy.process.message.model.UpdateResponderCommand;
import com.redhat.cajun.navy.rules.model.Mission;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class SetResponderUnavailableCommandBuilder {

    public static Pair<String, UpdateResponderCommand> builder(Map<String, Object> parameters) {

        Object payload = parameters.get("Payload");
        if (!(payload instanceof Mission)) {
            throw new IllegalStateException("Parameter 'payload' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Mission");
        }
        Mission mission = (Mission) payload;
        Responder responder = new Responder.Builder(mission.getResponderId()).available(false).build();
        return new ImmutablePair<>(mission.getResponderId(), new UpdateResponderCommand.Builder(responder).build());
    }


}
