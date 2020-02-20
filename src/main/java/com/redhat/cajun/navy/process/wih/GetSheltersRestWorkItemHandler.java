package com.redhat.cajun.navy.process.wih;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.redhat.cajun.navy.rules.model.Destination;
import com.redhat.cajun.navy.rules.model.Destinations;

import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.api.runtime.process.WorkItemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

@Component("DisasterService")
public class GetSheltersRestWorkItemHandler implements WorkItemHandler {

    private static Logger log = LoggerFactory.getLogger(GetSheltersRestWorkItemHandler.class);

    @Value("${disaster.service.scheme}")
    private String disasterServiceScheme;

    @Value("${disaster.service.url}")
    private String disasterServiceUrl;

    @Value("${disaster.service.shelters-path}")
    private String sheltersPath;


    @Override
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {

        RestTemplate restTemplate = new RestTemplate();
        Destinations destinations;
        try {
            UriComponents uriComponents = UriComponentsBuilder.newInstance().scheme(disasterServiceScheme)
                    .host(disasterServiceUrl).path(sheltersPath).build();
            ResponseEntity<List<Destination>> entity = restTemplate.exchange(uriComponents.toUriString(),
                    HttpMethod.GET, null, new ParameterizedTypeReference<List<Destination>>(){});
            destinations = new Destinations(entity.getBody());
        } catch (HttpClientErrorException e) {
            log.error("Http Exception when calling disaster service - response code : " + e.getRawStatusCode(), e);
            destinations = new Destinations();
        }
        Map<String, Object> results = new HashMap<>();
        results.put("destinations", destinations);
        manager.completeWorkItem(workItem.getId(), results);
    }

    @Override
    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {

    }
}
