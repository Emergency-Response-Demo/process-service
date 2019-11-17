package com.redhat.cajun.navy.process.wih;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
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
import java.util.List;
import java.util.Map;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.redhat.cajun.navy.rules.model.Responder;
import com.redhat.cajun.navy.rules.model.Responders;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

public class GetRespondersRestWorkItemHandlerTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Mock
    private WorkItem workItem;

    @Mock
    private WorkItemManager workItemManager;

    @Captor
    private ArgumentCaptor<Map<String, Object>> resultsCaptor;

    private GetRespondersRestWorkItemHandler wih;

    @Before
    public void beforeTest() {
        initMocks(this);
        wih = new GetRespondersRestWorkItemHandler();
        setField(wih, "responderServiceScheme", "http", null);
        setField(wih, "responderServiceUrl", "localhost:" + wireMockRule.port(), null);
        setField(wih, "availableRespondersPath", "/responders/available", null);
        setField(wih, "availableRespondersLimit", 100, null);
        when(workItem.getId()).thenReturn(1L);
    }

    @Test
    public void testWorkItemHandler() throws Exception {

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("response-service-response.json");
        stubFor(get(urlEqualTo("/responders/available?limit=100")).willReturn(
                aResponse().withStatus(200).withHeader("Content-type", "application/json")
                        .withBody(IOUtils.toString(is, Charset.defaultCharset()))));

        wih.executeWorkItem(workItem, workItemManager);
        verify(getRequestedFor(urlEqualTo("/responders/available?limit=100")));
        verify(workItemManager).completeWorkItem(eq(1L), resultsCaptor.capture());
        Map<String, Object> results = resultsCaptor.getValue();
        assertThat(results, notNullValue());
        assertThat(results.get("Responders"), notNullValue());
        assertThat(results.get("Responders") instanceof Responders, is(true));
        Responders responders = (Responders) results.get("Responders");
        List<Responder> responderList = responders.getResponders();
        assertThat(responderList, notNullValue());
        assertThat(responderList.size(), equalTo(2));
        Responder responder1 = responderList.get(0);
        assertThat(responder1.getId(), anyOf(equalTo("1"), equalTo("2")));
        assertResponder(responder1);
        Responder responder2 = responderList.get(1);
        assertThat(responder2.getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(responder2.getId(), not(equalTo(responder1.getId())));
        assertResponder(responder2);
    }

    private void assertResponder(Responder responder) {
        if (responder.getId().equals("1")) {
            assertThat(responder.getFullname(), equalTo("John Doe"));
            assertThat(responder.getPhoneNumber(), equalTo("111-222-333"));
            assertThat(responder.getLatitude(), equalTo(new BigDecimal("30.12345")));
            assertThat(responder.getLongitude(), equalTo(new BigDecimal("-70.98765")));
            assertThat(responder.getBoatCapacity(), equalTo(3));
            assertThat(responder.getHasMedical(), is(true));
            assertThat(responder.getPerson(), is(false));
        } else {
            assertThat(responder.getFullname(), equalTo("John Foo"));
            assertThat(responder.getPhoneNumber(), equalTo("999-888-777"));
            assertThat(responder.getLatitude(), equalTo(new BigDecimal("35.12345")));
            assertThat(responder.getLongitude(), equalTo(new BigDecimal("-75.98765")));
            assertThat(responder.getBoatCapacity(), equalTo(2));
            assertThat(responder.getHasMedical(), is(false));
            assertThat(responder.getPerson(), is(true));
        }
    }

}
