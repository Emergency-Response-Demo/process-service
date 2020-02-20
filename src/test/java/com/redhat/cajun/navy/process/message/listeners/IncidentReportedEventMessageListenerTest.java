package com.redhat.cajun.navy.process.message.listeners;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;
import static org.mockito.ArgumentMatchers.eq;
import static org.hamcrest.CoreMatchers.is;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Map;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.redhat.cajun.navy.process.wih.GetSheltersRestWorkItemHandler;
import com.redhat.cajun.navy.rules.model.Destination;
import com.redhat.cajun.navy.rules.model.Destinations;
import com.redhat.cajun.navy.rules.model.Incident;

import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.jbpm.process.instance.ProcessInstance;
import org.jbpm.services.api.ProcessService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.kie.internal.process.CorrelationKey;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

public class IncidentReportedEventMessageListenerTest {

    private IncidentReportedEventMessageListener messageListener;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Mock
    private WorkItem workItem;

    @Mock
    private WorkItemManager workItemManager;

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
    private ArgumentCaptor<String> processIdCaptor;

    @Captor
    private ArgumentCaptor<CorrelationKey> correlationKeyCaptor;

    @Captor
    private ArgumentCaptor<Map<String, Object>> resultsCaptor;

    @Captor
    private ArgumentCaptor<Map<String, Object>> parametersCaptor;

    private GetSheltersRestWorkItemHandler wih;

    private String processId = "incident";

    @Before
    public void init() {
        initMocks(this);
        messageListener = new IncidentReportedEventMessageListener();
        setField(messageListener, null, ptm, PlatformTransactionManager.class);
        setField(messageListener, null, processService, ProcessService.class);
        setField(messageListener, "processId", processId, String.class);
        setField(messageListener, "assignmentDelay", "PT30S", String.class);

        wih = new GetSheltersRestWorkItemHandler();
        setField(wih, "disasterServiceScheme", "http", null);
        setField(wih, "disasterServiceUrl", "localhost:" + wireMockRule.port(), null);
        setField(wih, "sheltersPath", "/shelters", null);

        when(ptm.getTransaction(any())).thenReturn(transactionStatus);
        when(processService.startProcess(any(), any(), any(), any())).thenReturn(100L);
    }

    @Test
    public void testProcessIncidentReportedEventMessage() throws Exception {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("disaster-service-response.json");
        stubFor(get(urlEqualTo("/shelters")).willReturn(
                aResponse().withStatus(200).withHeader("Content-type", "application/json")
                        .withBody(IOUtils.toString(is, Charset.defaultCharset()))));

        String json = "{\"messageType\":\"IncidentReportedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\": {\"id\":\"incident123\"," +
                "\"lat\": \"34.14338\"," +
                "\"lon\": \"-77.86569\"," +
                "\"numberOfPeople\": 3," +
                "\"medicalNeeded\": true," +
                "\"timestamp\": 1521148332350," +
                "\"victimName\":\"John Doe\"," +
                "\"victimPhoneNumber\":\"111-222-333\"," +
                "\"status\":\"REPORTED\"" +
                "}}";

        messageListener.processMessage(json, "incident123", "topic1", 1, ack);
        
        verify(processService).startProcess(any(), processIdCaptor.capture(), correlationKeyCaptor.capture(), parametersCaptor.capture());
        assertThat(processIdCaptor.getValue(), equalTo(processId));
        CorrelationKey correlationKey = correlationKeyCaptor.getValue();
        assertThat(correlationKey.getName(), equalTo("incident123"));
        Map<String, Object> parameters = parametersCaptor.getValue();
        assertThat(parameters.size(), equalTo(2));
        assertThat(parameters.get("assignmentDelay"), equalTo("PT30S"));
        assertThat(parameters.get("incident"), notNullValue());
        assertThat(parameters.get("incident") instanceof Incident, CoreMatchers.equalTo(true));
        Incident incident = (Incident) parameters.get("incident");
        assertThat(incident.getId(), equalTo("incident123"));
        assertThat(incident.getLatitude(), equalTo(new BigDecimal("34.14338")));
        assertThat(incident.getLongitude(), equalTo(new BigDecimal("-77.86569")));
        assertThat(incident.getNumPeople(), equalTo(3));
        assertThat(incident.getMedicalNeeded(), equalTo(true));
        assertThat(incident.getReportedTime(), equalTo(1521148332350l));

        wih.executeWorkItem(workItem, workItemManager);
        verify(getRequestedFor(urlEqualTo("/shelters")));
        verify(workItemManager).completeWorkItem(eq(0L), resultsCaptor.capture());
        Map<String, Object> results = resultsCaptor.getValue();
        assertThat(results, notNullValue());
        assertThat(results.get("destinations"), notNullValue());
        assertThat(results.get("destinations") instanceof Destinations, is(true));
        Destinations destinations = (Destinations) results.get("destinations");
        assertThat(destinations, notNullValue());
        assertThat(destinations.getDestinations().size(), equalTo(3));
        Destination destination1 = destinations.getDestinations().get(0);
        assertThat(destination1.getName(), equalTo("Port City Marina"));
        assertThat(destination1.getLatitude(), equalTo(new BigDecimal("34.24609")));
        assertThat(destination1.getLongitude(), equalTo(new BigDecimal("-77.95189")));
        Destination destination2 = destinations.getDestinations().get(1);
        assertThat(destination2.getName(), equalTo("Wilmington Marine Center"));
        assertThat(destination2.getLatitude(), equalTo(new BigDecimal("34.17060")));
        assertThat(destination2.getLongitude(), equalTo(new BigDecimal("-77.94899")));
        Destination destination3 = destinations.getDestinations().get(2);
        assertThat(destination3.getName(), equalTo("Carolina Beach Yacht Club"));
        assertThat(destination3.getLatitude(), equalTo(new BigDecimal("34.05830")));
        assertThat(destination3.getLongitude(), equalTo(new BigDecimal("-77.88849")));

        verify(ack).acknowledge();
    }
}
