package com.redhat.cajun.navy.process.outbox;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import io.cloudevents.CloudEvent;
import io.cloudevents.types.Time;
import org.springframework.stereotype.Component;

@Component
public class OutboxEventEmitter {

    @PersistenceContext
    private EntityManager entityManager;

    public void emitEvent(OutboxEvent event) {
        entityManager.persist(event);
        entityManager.remove(event);
    }

    public void emitCloudEvent(CloudEvent cloudEvent) {
        OutboxEvent event = new OutboxEvent();
        event.setAggregateId((String) cloudEvent.getExtension("aggregateid"));
        event.setAggregateType((String) cloudEvent.getExtension("aggregatetype"));
        event.setType(cloudEvent.getType());
        event.setPayload(new String(cloudEvent.getData().toBytes()));
        event.setCeDataContentType(cloudEvent.getDataContentType());
        event.setCeSource(cloudEvent.getSource().toString());
        event.setCeTime(Time.writeTime(cloudEvent.getTime()));
        if (cloudEvent.getExtension("incidentid") != null) {
            event.setCeIncidentId((String) cloudEvent.getExtension("incidentid"));
        } else {
            event.setCeIncidentId("");
        }
        event.setCeSpecVersion(cloudEvent.getSpecVersion().toString());
        emitEvent(event);
    }

}