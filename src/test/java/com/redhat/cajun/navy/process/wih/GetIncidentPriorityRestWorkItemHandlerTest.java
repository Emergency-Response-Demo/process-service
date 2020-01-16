package com.redhat.cajun.navy.process.wih;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.math.BigDecimal;
import java.util.Map;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.redhat.cajun.navy.process.message.model.Incident.IncidentStatus;
import com.redhat.cajun.navy.rules.model.Incident;
import com.redhat.cajun.navy.rules.model.IncidentPriority;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

public class GetIncidentPriorityRestWorkItemHandlerTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Mock
    private WorkItem workItem;

    @Mock
    private WorkItemManager workItemManager;

    @Captor
    private ArgumentCaptor<Map<String, Object>> resultsCaptor;

    private GetIncidentPriorityRestWorkItemHandler wih;

    @Before
    public void beforeTest() {
        initMocks(this);
        wih = new GetIncidentPriorityRestWorkItemHandler();
        ReflectionTestUtils.setField(wih, "serviceScheme", "http", null);
        ReflectionTestUtils.setField(wih, "serviceUrl", "localhost:" + wireMockRule.port(), null);
        ReflectionTestUtils.setField(wih, "incidentPriorityPath", "/priority/{incidentId}", null);
        when(workItem.getId()).thenReturn(1L);
    }


    @Test
    public void testWorkItemHandler() throws Exception {

        String ip = "{" + "\"incidentId\": \"incident123\"," + "\"priority\": 1,"
                + "\"average\": 2.5," + "\"incidents\": 3" +"}";

        stubFor(post(urlEqualTo("/priority/incident123")).willReturn(
                aResponse().withStatus(200).withHeader("Content-type", "application/json")
                        .withBody(ip)));


        Incident incident = new Incident();
        incident.setId("incident123");
        incident.setLatitude(BigDecimal.ZERO);
        incident.setLongitude(BigDecimal.ZERO);
        incident.setStatus(IncidentStatus.REPORTED.name());

        when(workItem.getParameter("Incident")).thenReturn(incident);

        wih.executeWorkItem(workItem, workItemManager);
        verify(postRequestedFor(urlEqualTo("/priority/incident123")));
        Mockito.verify(workItemManager).completeWorkItem(eq(1L), resultsCaptor.capture());
        Map<String, Object> results = resultsCaptor.getValue();
        assertThat(results, notNullValue());
        assertThat(results.get("IncidentPriority"), notNullValue());
        assertThat(results.get("IncidentPriority") instanceof IncidentPriority, is(true));
        IncidentPriority incidentPriority = (IncidentPriority) results.get("IncidentPriority");
        assertThat(incidentPriority.getIncidentId(), equalTo("incident123"));
        assertThat(incidentPriority.getPriority(), equalTo(new BigDecimal(1)));
        assertThat(incidentPriority.getAveragePriority(), equalTo(new BigDecimal(2.5)));
        assertThat(incidentPriority.getIncidents(), equalTo(new BigDecimal(3)));
    }


}
