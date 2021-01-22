package com.redhat.cajun.navy.process.wih;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Map;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.redhat.cajun.navy.rules.model.Destination;
import com.redhat.cajun.navy.rules.model.Destinations;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

public class GetSheltersRestWorkItemHandlerTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Mock
    private WorkItem workItem;

    @Mock
    private WorkItemManager workItemManager;

    @Captor
    private ArgumentCaptor<Map<String, Object>> resultsCaptor;

    private GetSheltersRestWorkItemHandler wih;

    @Before
    public void init() {
        initMocks(this);
        wih = new GetSheltersRestWorkItemHandler();
        setField(wih, "disasterServiceScheme", "http", null);
        setField(wih, "disasterServiceUrl", "localhost:" + wireMockRule.port(), null);
        setField(wih, "sheltersPath", "/shelters", null);
        when(workItem.getId()).thenReturn(1L);
    }

    @Test
    public void testWorkItemHandler() throws Exception {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("disaster-service-response.json");
        assert is != null;
        stubFor(get(urlEqualTo("/shelters")).willReturn(
                aResponse().withStatus(200).withHeader("Content-type", "application/json")
                        .withBody(IOUtils.toString(is, Charset.defaultCharset()))));

        wih.executeWorkItem(workItem, workItemManager);
        verify(getRequestedFor(urlEqualTo("/shelters")));
        verify(workItemManager).completeWorkItem(eq(1L), resultsCaptor.capture());
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
    }
}
