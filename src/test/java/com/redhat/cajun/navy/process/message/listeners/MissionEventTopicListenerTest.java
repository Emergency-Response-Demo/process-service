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

import java.util.Collections;

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

        String json = "{" + "\"messageType\" : \"MissionStartedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("MissionStarted"));

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService).signalProcessInstance(100L, "MissionStarted", null);

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionStartedEventMessageWhenNotFound() {

        String json = "{" + "\"messageType\" : \"MissionStartedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(null);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("MissionStarted"));

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMissionStartedEventMessageWhenNoIncidentId() {

        String json = "{" + "\"messageType\" : \"MissionStartedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionPickedUpEventMessage() {

        String json = "{" + "\"messageType\" : \"MissionPickedUpEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"" +
                "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("VictimPickedUp"));

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService).signalProcessInstance(100L, "VictimPickedUp", null);

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionPickedUpEventMessageWhenNotFound() {

        String json = "{" + "\"messageType\" : \"MissionPickedUpEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(null);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("VictimPickedUp"));

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessVictimPickedUpEventMessageWhenNoIncidentId() {

        String json = "{" + "\"messageType\" : \"VictimPickedUpEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionCompletedEventMessage() {

        String json = "{" + "\"messageType\" : \"MissionCompletedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("VictimDelivered"));

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService).signalProcessInstance(100L, "VictimDelivered", null);

        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMissionCompletedEventMessageWhenNotFound() {

        String json = "{" + "\"messageType\" : \"MissionCompletedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"incidentId\" : \"incident123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(null);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("VictimPickedUp"));

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessVictimDeliveredEventMessageWhenNoIncidentId() {

        String json = "{" + "\"messageType\" : \"VictimDeliveredEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWhenWrongMessageType() {

        String json = "{" + "\"messageType\" : \"WrongMessageTypet\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\" : {" +
                "\"missionId\" : \"mission123\"," +
                "\"responderId\" : \"responder123\"," +
                "\"responderStartLat\" : \"30.12345\"," +
                "\"responderStartLong\" : \"-77.98765\"," +
                "\"incidentLat\" : \"31.12345\"," +
                "\"incidentLong\" : \"-78.98765\"," +
                "\"destinationLat\" : \"32.12345\"," +
                "\"destinationLong\" : \"-79.98765\"" +
                "}" + "}";

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWhenWrongMessageStructure() {

        String json = "{" + "\"field1\" : \"value1\"," +
                "\"field2\":\"value2\"" +
                "}";

        messageListener.processMessage(json, "topic", 1, ack);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());

        verify(ack).acknowledge();
    }

}
