package com.redhat.cajun.navy.process.message.listeners;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import org.jbpm.process.instance.ProcessInstance;
import org.jbpm.services.api.ProcessService;
import org.junit.Before;
import org.junit.Test;
import org.kie.internal.process.CorrelationKey;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
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

    @Captor
    private ArgumentCaptor<CorrelationKey> correlationCaptor;

    private MissionEventTopicListener messageListener;

    @Before
    public void init() {
        initMocks(this);
        messageListener = new MissionEventTopicListener();
        setField(messageListener, null, ptm, PlatformTransactionManager.class);
        setField(messageListener, null, processService, ProcessService.class);
        when(ptm.getTransaction(any())).thenReturn(transactionStatus);
        when(processInstance.getId()).thenReturn(100L);
    }

    @Test
    public void testProcessMessage() {

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

        messageListener.processMessage(json);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService).signalProcessInstance(100L, "MissionStarted", null);
    }

    @Test
    public void testProcessMessageWhenNotFound() {

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

        messageListener.processMessage(json);

        verify(processService).getProcessInstance(correlationCaptor.capture());
        CorrelationKey correlationKey = correlationCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(processService, never()).signalProcessInstance(any(), any(), any());
    }

    @Test
    public void testProcessMessageWhenNoIncidentId() {

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

        messageListener.processMessage(json);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());
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

        messageListener.processMessage(json);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());
    }

    @Test
    public void testProcessMessageWhenWrongMessageStructure() {

        String json = "{" + "\"field1\" : \"value1\"," +
                "\"field2\":\"calue2\"" +
                "}";

        messageListener.processMessage(json);

        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));
        verify(processService, never()).signalProcessInstance(any(), any(), any());
    }

}
