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

public class ResponderUpdatedEventMessageListenerTest {

    @Mock
    private PlatformTransactionManager ptm;

    @Mock
    private TransactionStatus transactionStatus;

    @Mock
    private ProcessService processService;

    @Mock
    private ProcessInstance processInstance;

    @Mock
    private Acknowledgment ack;

    @Mock
    private QueryService queryService;

    @Captor
    private ArgumentCaptor<CorrelationKey> correlationKeyCaptor;

    private ResponderUpdatedEventMessageListener messageListener;

    @Before
    public void init() {
        initMocks(this);
        messageListener = new ResponderUpdatedEventMessageListener();
        setField(messageListener, null, ptm, PlatformTransactionManager.class);
        setField(messageListener, null, processService, ProcessService.class);
        setField(messageListener, null, queryService, QueryService.class);
        when(ptm.getTransaction(any())).thenReturn(transactionStatus);
        when(processInstance.getId()).thenReturn(100L);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMessage() {
        String json = "{" + "\"messageType\" : \"ResponderUpdatedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"header\" : {\"incidentId\" : \"incident123\"}," +
                "\"body\" : {" +
                "\"status\" : \"success\"," +
                "\"responder\" : {" +
                "\"id\" : \"responderId\"," +
                "\"name\" : \"John Doe\"," +
                "\"phoneNumber\" : \"111-222-333\"," +
                "\"latitude\" : 30.12345," +
                "\"longitude\" : -70.12345," +
                "\"boatCapacity\" : 2," +
                "\"medicalKit\" : true," +
                "\"available\" : false" +
                "}" + "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("ResponderAvailable"));

        messageListener.processMessage(json, "responderId", "test-topic", 1, ack);

        verify(processService).signalProcessInstance(100L, "ResponderAvailable", true);
        verify(processService).getProcessInstance(correlationKeyCaptor.capture());
        CorrelationKey correlationKey = correlationKeyCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMessageWithPersonField() {
        String json = "{" + "\"messageType\" : \"ResponderUpdatedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"header\" : {\"incidentId\" : \"incident123\"}," +
                "\"body\" : {" +
                "\"status\" : \"success\"," +
                "\"responder\" : {" +
                "\"id\" : \"responderId\"," +
                "\"name\" : \"John Doe\"," +
                "\"phoneNumber\" : \"111-222-333\"," +
                "\"latitude\" : 30.12345," +
                "\"longitude\" : -70.12345," +
                "\"boatCapacity\" : 2," +
                "\"medicalKit\" : true," +
                "\"available\" : false," +
                "\"person\" : false" +
                "}" + "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("ResponderAvailable"));

        messageListener.processMessage(json, "responderId", "test-topic", 1, ack);

        verify(processService).signalProcessInstance(100L, "ResponderAvailable", true);
        verify(processService).getProcessInstance(correlationKeyCaptor.capture());
        CorrelationKey correlationKey = correlationKeyCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMessageWhenStatusError() {
        String json = "{" + "\"messageType\" : \"ResponderUpdatedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"header\" : {\"incidentId\" : \"incident123\"}," +
                "\"body\" : {" +
                "\"status\" : \"error\"," +
                "\"responder\" : {" +
                "\"id\" : \"responderId\"," +
                "\"name\" : \"John Doe\"," +
                "\"phoneNumber\" : \"111-222-333\"," +
                "\"latitude\" : 30.12345," +
                "\"longitude\" : -70.12345," +
                "\"boatCapacity\" : 2," +
                "\"medicalKit\" : true," +
                "\"available\" : false" +
                "}" + "}" + "}";

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);
        when(queryService.query(anyString(), any(QueryResultMapper.class), any(QueryContext.class), any(QueryParam.class)))
                .thenReturn(Collections.singletonList("ResponderAvailable"));

        messageListener.processMessage(json, "responderId", "test-topic", 1, ack);

        verify(processService).signalProcessInstance(100L, "ResponderAvailable", false);
        verify(processService).getProcessInstance(correlationKeyCaptor.capture());
        CorrelationKey correlationKey = correlationKeyCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWhenNoHeader() {
        String json = "{" + "\"messageType\" : \"ResponderUpdatedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"header\" : {\"wrongHeader\" : \"incident123\"}," +
                "\"body\" : {" +
                "\"status\" : \"error\"," +
                "\"responder\" : {" +
                "\"id\" : \"responderId\"," +
                "\"name\" : \"John Doe\"," +
                "\"phoneNumber\" : \"111-222-333\"," +
                "\"latitude\" : 30.12345," +
                "\"longitude\" : -70.12345," +
                "\"boatCapacity\" : 2," +
                "\"medicalKit\" : true," +
                "\"available\" : false" +
                "}" + "}" + "}";

        messageListener.processMessage(json, "responderId", "test-topic", 1, ack);

        verify(processService, never()).signalProcessInstance(any(), any(), any());
        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWrongMessageType() {
        String json = "{" + "\"messageType\" : \"WrongMessageType\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"header\" : {\"incidentId\" : \"incident123\"}," +
                "\"body\" : {" +
                "\"status\" : \"success\"," +
                "\"responder\" : {" +
                "\"id\" : \"responderId\"," +
                "\"name\" : \"John Doe\"," +
                "\"phoneNumber\" : \"111-222-333\"," +
                "\"latitude\" : 30.12345," +
                "\"longitude\" : -70.12345," +
                "\"boatCapacity\" : 2," +
                "\"medicalKit\" : true," +
                "\"available\" : false" +
                "}" + "}" + "}";

        messageListener.processMessage(json, "responderId", "test-topic", 1, ack);

        verify(processService, never()).signalProcessInstance(any(), any(), any());
        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWrongMessageStructure() {
        String json = "{" + "\"field1\" : \"value1\"," +
                "\"field2\":\"calue2\"" +
                "}";

        messageListener.processMessage(json, "responderId", "test-topic", 1, ack);

        verify(processService, never()).signalProcessInstance(any(), any(), any());
        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));

        verify(ack).acknowledge();
    }




}
