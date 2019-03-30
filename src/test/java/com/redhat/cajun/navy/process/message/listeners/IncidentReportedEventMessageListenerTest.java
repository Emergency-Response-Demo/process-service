package com.redhat.cajun.navy.process.message.listeners;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import java.math.BigDecimal;
import java.util.Map;

import com.redhat.cajun.navy.process.message.model.DestinationLocations;
import com.redhat.cajun.navy.rules.model.Destination;
import com.redhat.cajun.navy.rules.model.Destinations;
import com.redhat.cajun.navy.rules.model.Incident;
import org.hamcrest.CoreMatchers;
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

public class IncidentReportedEventMessageListenerTest {

    private IncidentReportedEventMessageListener messageListener;

    @Mock
    private PlatformTransactionManager ptm;

    @Mock
    private TransactionStatus transactionStatus;

    @Mock
    private ProcessService processService;

    @Mock
    private ProcessInstance processInstance;

    @Captor
    private ArgumentCaptor<String> processIdCaptor;

    @Captor
    private ArgumentCaptor<CorrelationKey> correlationKeyCaptor;

    @Captor
    private ArgumentCaptor<Map<String, Object>> parametersCaptor;

    private String processId = "incident";

    @Before
    public void init() {
        initMocks(this);
        messageListener = new IncidentReportedEventMessageListener();
        setField(messageListener, null, ptm, PlatformTransactionManager.class);
        setField(messageListener, null, processService, ProcessService.class);
        setField(messageListener, "processId", processId, String.class);
        setField(messageListener, "destinationLocations", destinationLocations(), DestinationLocations.class);
        when(ptm.getTransaction(any())).thenReturn(transactionStatus);
        when(processService.startProcess(any(), any(), any(), any())).thenReturn(100L);

    }

    @Test
    public void testProcessIncidentReportedEventMessage() {
        String json = "{\"messageType\":\"IncidentReportedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\": {\"id\":\"incident123\"," +
                "\"lat\": \"34.14338\"," +
                "\"lon\": \"-77.86569\"," +
                "\"numberOfPeople\": 3," +
                "\"medicalNeeded\": true," +
                "\"timestamp\": 1521148332350" +
                "}}";

        messageListener.processMessage(json, "incident123", "topic1", 1);


        verify(processService).startProcess(any(), processIdCaptor.capture(), correlationKeyCaptor.capture(), parametersCaptor.capture());
        assertThat(processIdCaptor.getValue(), equalTo(processId));
        CorrelationKey correlationKey = correlationKeyCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        Map<String, Object> parameters = parametersCaptor.getValue();
        assertThat(parameters.size(), equalTo(2));
        assertThat(parameters.get("incident"), notNullValue());
        assertThat(parameters.get("incident") instanceof Incident, CoreMatchers.equalTo(true));
        Incident incident = (Incident) parameters.get("incident");
        assertThat(incident.getId(), equalTo("incident123"));
        assertThat(incident.getLatitude(), equalTo(new BigDecimal("34.14338")));
        assertThat(incident.getLongitude(), equalTo(new BigDecimal("-77.86569")));
        assertThat(incident.getNumPeople(), equalTo(3));
        assertThat(incident.getMedicalNeeded(), equalTo(true));
        assertThat(incident.getReportedTime(), equalTo(1521148332350l));
        Destinations destinations = (Destinations) parameters.get("destinations");
        assertThat(destinations, notNullValue());
        assertThat(destinations.getDestinations().size(), equalTo(2));
        Destination destination1 = destinations.getDestinations().get(0);
        assertThat(destination1.getName(), equalTo("loc1"));
        assertThat(destination1.getLatitude(), equalTo(new BigDecimal("20.12345")));
        assertThat(destination1.getLongitude(), equalTo(new BigDecimal("-50.98765")));
        Destination destination2 = destinations.getDestinations().get(1);
        assertThat(destination2.getName(), equalTo("loc2"));
        assertThat(destination2.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(destination2.getLongitude(), equalTo(new BigDecimal("-60.98765")));
    }

    private DestinationLocations destinationLocations() {
        DestinationLocations dl = new DestinationLocations();
        DestinationLocations.Location loc1 = new DestinationLocations.Location();
        loc1.setName("loc1");
        loc1.setLatitude("20.12345");
        loc1.setLongitude("-50.98765");
        dl.getLocations().add(loc1);
        DestinationLocations.Location loc2 = new DestinationLocations.Location();
        loc2.setName("loc2");
        loc2.setLatitude("30.12345");
        loc2.setLongitude("-60.98765");
        dl.getLocations().add(loc2);
        return dl;
    }
}
