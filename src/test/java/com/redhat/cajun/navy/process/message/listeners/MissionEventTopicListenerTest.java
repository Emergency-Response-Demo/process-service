package com.redhat.cajun.navy.process.message.listeners;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import java.net.URI;
import java.util.Collections;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jbpm.process.instance.ProcessInstance;
import org.jbpm.services.api.ProcessService;
import org.jbpm.services.api.query.QueryResultMapper;
import org.jbpm.services.api.query.QueryService;
import org.jbpm.services.api.query.model.QueryParam;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.runtime.query.QueryContext;
import org.kie.internal.process.CorrelationKey;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

public class MissionEventTopicListenerTest {

    @Mock
    private PlatformTransactionManager ptm;

    @Mock
    private TransactionStatus transactionStatus;

    @Mock
    private ProcessService processService;

    @Mock
    private ProcessInstance processInstance;

    @Mock
    private QueryService queryService;

    @Mock
    private Acknowledgment ack;

    @Captor
    private ArgumentCaptor<CorrelationKey> correlationCaptor;

    private MissionEventTopicListener messageListener;

    @Before
    public void init() {
        initMocks(this);
        messageListener = new MissionEventTopicListener();
        setField(messageListener, null, ptm, PlatformTransactionManager.class);
        setField(messageListener, null, processService, ProcessService.class);
        setField(messageListener, null, queryService, QueryService.class);
        when(ptm.getTransaction(any())).thenReturn(transactionStatus);
        when(processInstance.getId()).thenReturn(100L);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionStartedEventMessage() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionStartedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("MissionStarted"));

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService).signalProcessInstance(100L, "MissionStarted", null);

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionStartedEventMessageWhenNotFound() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionStartedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(null);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("MissionStarted"));

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMissionStartedEventMessageWhenNoIncidentId() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionStartedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionPickedUpEventMessage() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionPickedUpEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("VictimPickedUp"));

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService).signalProcessInstance(100L, "VictimPickedUp", null);

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionPickedUpEventMessageWhenNotFound() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionPickedUpEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(null);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("VictimPickedUp"));

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMissionPickedUpEventMessageWhenNoIncidentId() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionPickedUpEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionCompletedEventMessage() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionCompletedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("VictimDelivered"));

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService).signalProcessInstance(100L, "VictimDelivered", null);

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionCompletedEventMessageWhenNotFound() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionCompletedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(null);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("VictimPickedUp"));

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessVictimDeliveredEventMessageWhenNoIncidentId() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionCompletedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWhenWrongMessageType() {

        String json = "{" +
                "\"missionId\" : \"mission123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("WrongMessageType")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWhenWrongMessageStructure() {

        String json = "{" + "\"field1\" : \"value1\"," +
                "\"field2\":\"value2\"" +
                "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("MissionStartedUpEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        messageListener.processMessage(event, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

}
