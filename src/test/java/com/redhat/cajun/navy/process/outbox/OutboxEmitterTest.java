package com.redhat.cajun.navy.process.outbox;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import java.time.OffsetDateTime;
import java.util.UUID;
import javax.persistence.EntityManager;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.cajun.navy.process.message.model.CloudEventBuilder;
import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.types.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

public class OutboxEmitterTest {

    @Mock
    private EntityManager entityManager;

    @Captor
    private ArgumentCaptor<OutboxEvent> outboxEventCaptor;

    OutboxEventEmitter outboxEventEmitter;

    @Before
    public void setup() {
        initMocks(this);
        outboxEventEmitter = new OutboxEventEmitter();
        setField(outboxEventEmitter, null, entityManager, EntityManager.class);
    }

    @Test
    public void testEmitCloudEvent() throws Exception {
        TestEvent testEvent = TestEvent.build();
        CloudEvent cloudEvent = new CloudEventBuilder<TestEvent>()
                .withType("TestEvent")
                .withExtension("incidentid", "incident123")
                .withExtension("aggregatetype", "responder-command")
                .withExtension("aggregateid", "responder123")
                .withData(testEvent)
                .build();

        outboxEventEmitter.emitCloudEvent(cloudEvent);
        verify(entityManager).persist(outboxEventCaptor.capture());
        verify(entityManager).remove(outboxEventCaptor.capture());

        assertThat(outboxEventCaptor.getAllValues().size(), equalTo(2));
        OutboxEvent event = outboxEventCaptor.getAllValues().get(0);
        assertThat(event.getAggregateType(), equalTo("responder-command"));
        assertThat(event.getAggregateId(), equalTo("responder123"));
        assertThat(event.getType(), equalTo("TestEvent"));
        assertThat(event.getPayload(), equalTo(new ObjectMapper().writeValueAsString(testEvent)));
        assertThat(event.getCeSpecVersion(), equalTo(SpecVersion.V1.toString()));
        assertThat(event.getCeDataContentType(), equalTo("application/json"));
        assertThat(event.getCeSource(), equalTo("emergency-response/process-service"));
        assertThat(event.getCeIncidentId(), equalTo("incident123"));
        assertThat(event.getCeTime(), notNullValue());
        OffsetDateTime offsetDateTime = Time.parseTime(event.getCeTime());
        assertThat(offsetDateTime, notNullValue());
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class TestEvent {

        public String id;

        static TestEvent build() {
            TestEvent event = new TestEvent();
            event.id = UUID.randomUUID().toString();
            return event;
        }

    }
}
