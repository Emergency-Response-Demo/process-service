package com.redhat.cajun.navy.process.message.listeners;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import java.net.URI;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jbpm.process.instance.ProcessInstance;
import org.jbpm.services.api.ProcessService;
import org.junit.Before;
import org.junit.Test;
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

    @Captor
    private ArgumentCaptor<CorrelationKey> correlationKeyCaptor;

    private ResponderUpdatedEventMessageListener messageListener;

    @Before
    public void init() {
        initMocks(this);
        messageListener = new ResponderUpdatedEventMessageListener();
        setField(messageListener, null, ptm, PlatformTransactionManager.class);
        setField(messageListener, null, processService, ProcessService.class);
        when(ptm.getTransaction(any())).thenReturn(transactionStatus);
        when(processInstance.getId()).thenReturn(100L);
    }

    @Test
    public void testProcessMessage() {
        String json = "{" +
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
                "}" + "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("ResponderUpdatedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .withExtension("incidentid", "incident123")
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);

        messageListener.processMessage(event, "responderId", "test-topic", 1, ack);

        verify(processService).signalProcessInstance(100L, "ResponderAvailable", true);
        verify(processService).getProcessInstance(correlationKeyCaptor.capture());
        CorrelationKey correlationKey = correlationKeyCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWithPersonField() {
        String json = "{" +
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
                "}" + "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("ResponderUpdatedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .withExtension("incidentid", "incident123")
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);

        messageListener.processMessage(event, "responderId", "test-topic", 1, ack);

        verify(processService).signalProcessInstance(100L, "ResponderAvailable", true);
        verify(processService).getProcessInstance(correlationKeyCaptor.capture());
        CorrelationKey correlationKey = correlationKeyCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWhenStatusError() {
        String json = "{" +
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
                "}" + "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("ResponderUpdatedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .withExtension("incidentid", "incident123")
                .build();

        when(processService.getProcessInstance(any(CorrelationKey.class))).thenReturn(processInstance);

        messageListener.processMessage(event, "responderId", "test-topic", 1, ack);

        verify(processService).signalProcessInstance(100L, "ResponderAvailable", false);
        verify(processService).getProcessInstance(correlationKeyCaptor.capture());
        CorrelationKey correlationKey = correlationKeyCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWhenNoIncidentIdExtension() {
        String json = "{" +
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
                "}" + "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("ResponderUpdatedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .build();

        messageListener.processMessage(event, "responderId", "test-topic", 1, ack);

        verify(processService, never()).signalProcessInstance(any(), any(), any());
        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWrongMessageType() {
        String json = "{" +
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
                "}" + "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("WrongMessageType")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .withExtension("incidentid", "incident123")
                .build();

        messageListener.processMessage(event, "responderId", "test-topic", 1, ack);

        verify(processService, never()).signalProcessInstance(any(), any(), any());
        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWrongDataContentType() {
        byte[] bytes = {1,2,3};

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("ResponderUpdatedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/binary")
                .withData(bytes)
                .withExtension("incidentid", "incident123")
                .build();

        messageListener.processMessage(event, "responderId", "test-topic", 1, ack);

        verify(processService, never()).signalProcessInstance(any(), any(), any());
        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageNoData() {

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("ResponderUpdatedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/binary")
                .withExtension("incidentid", "incident123")
                .build();

        messageListener.processMessage(event, "responderId", "test-topic", 1, ack);

        verify(processService, never()).signalProcessInstance(any(), any(), any());
        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));

        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageNotAnResponderUpdatedEvent() {

        String json = "{" + "\"field1\": \"value1\"," +
                "\"field2\": \"value2\"" + "}";

        CloudEvent event = CloudEventBuilder.v1()
                .withId("000")
                .withType("ResponderUpdatedEvent")
                .withSource(URI.create("http://example.com"))
                .withDataContentType("application/json")
                .withData(json.getBytes())
                .withExtension("incidentid", "incident123")
                .build();

        messageListener.processMessage(event, "responderId", "test-topic", 1, ack);

        verify(processService, never()).signalProcessInstance(any(), any(), any());
        verify(processService, never()).getProcessInstance(any(CorrelationKey.class));

        verify(ack).acknowledge();
    }

}
