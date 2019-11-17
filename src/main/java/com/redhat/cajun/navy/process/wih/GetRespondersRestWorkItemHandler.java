package com.redhat.cajun.navy.process.wih;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.redhat.cajun.navy.rules.model.Responders;
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

@Component("ResponderService")
public class GetRespondersRestWorkItemHandler implements WorkItemHandler {

    private static Logger log = LoggerFactory.getLogger(GetRespondersRestWorkItemHandler.class);

    @Value("${responder.service.scheme}")
    private String responderServiceScheme;

    @Value("${responder.service.url}")
    private String responderServiceUrl;

    @Value("${responder.service.available-responders-path}")
    private String availableRespondersPath;

    @Value("${responder.service.available-responders-limit}")
    private Integer availableRespondersLimit;


    @Override
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {

        RestTemplate restTemplate = new RestTemplate();
        Responders responders;
        try {
            UriComponents uriComponents = UriComponentsBuilder.newInstance().scheme(responderServiceScheme)
                    .host(responderServiceUrl).path(availableRespondersPath).query("limit="+availableRespondersLimit).build();
            ResponseEntity<List<Responder>> entity = restTemplate.exchange(uriComponents.toUriString(),
                    HttpMethod.GET, null, new ParameterizedTypeReference<List<Responder>>(){});
            responders = new Responders(entity.getBody().stream().map(r -> {
                com.redhat.cajun.navy.rules.model.Responder responder = new com.redhat.cajun.navy.rules.model.Responder();
                responder.setId(Long.toString(r.getId()));
                responder.setFullname(r.getName());
                responder.setPhoneNumber(r.getPhoneNumber());
                responder.setLatitude(r.getLatitude());
                responder.setLongitude(r.getLongitude());
                responder.setBoatCapacity(r.getBoatCapacity());
                responder.setHasMedical(r.isMedicalKit());
                responder.setPerson(r.isPerson());
                return responder;
            }).collect(Collectors.toList()));

        } catch (HttpClientErrorException e) {
            log.error("Http Exception when calling responder service - response code : " + e.getRawStatusCode(), e);
            responders = new Responders();
        }
        Map<String, Object> results = new HashMap<>();
        results.put("Responders", responders);
        manager.completeWorkItem(workItem.getId(), results);

    }

    @Override
    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {

    }

    public static class Responder {

        private long id;

        private String name;

        private String phoneNumber;

        private BigDecimal latitude;

        private BigDecimal longitude;

        private int boatCapacity;

        private boolean medicalKit;

        private boolean available;

        private boolean person;

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getPhoneNumber() {
            return phoneNumber;
        }

        public BigDecimal getLatitude() {
            return latitude;
        }

        public BigDecimal getLongitude() {
            return longitude;
        }

        public int getBoatCapacity() {
            return boatCapacity;
        }

        public boolean isMedicalKit() {
            return medicalKit;
        }

        public boolean isAvailable() {
            return available;
        }

        public boolean isPerson() {
            return person;
        }
    }
}
