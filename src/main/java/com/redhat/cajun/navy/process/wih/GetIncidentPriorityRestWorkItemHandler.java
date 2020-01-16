package com.redhat.cajun.navy.process.wih;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.redhat.cajun.navy.process.message.model.Incident.IncidentStatus;
import com.redhat.cajun.navy.rules.model.Incident;
import com.redhat.cajun.navy.rules.model.IncidentPriority;

import org.dashbuilder.json.Json;
import org.dashbuilder.json.JsonObject;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.api.runtime.process.WorkItemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Component("IncidentPriorityService")
public class GetIncidentPriorityRestWorkItemHandler implements WorkItemHandler {

    private static Logger log = LoggerFactory.getLogger(GetIncidentPriorityRestWorkItemHandler.class);

    @Value("${incident-priority.service.scheme}")
    private String serviceScheme;

    @Value("${incident-priority.service.url}")
    private String serviceUrl;

    @Value("${incident-priority.service.incident-priority-path}")
    private String incidentPriorityPath;

    @Override
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {

        Object incidentObj = workItem.getParameter("Incident");
        if (!(incidentObj instanceof Incident)) {
            throw new IllegalStateException("Parameter 'Incident' cannot be null and must be of type com.redhat.cajun.navy.rules.model.Incident");
        }
        Incident incident = (Incident) incidentObj;
        RestTemplate restTemplate = new RestTemplate();
        IncidentPriority incidentPriority;
        try {
            JsonObject json = Json.createObject();
            json.put("lat", incident.getLatitude().toString());
            json.put("lon", incident.getLongitude().toString());
            json.put("active", incident.getStatus() == null || incident.getStatus().equals(IncidentStatus.REPORTED.name()));
            HttpEntity<JsonObject> request = new HttpEntity<JsonObject>(json);
            RestIncidentPriority ip = restTemplate.exchange(serviceScheme + "://" + serviceUrl + incidentPriorityPath,
                    HttpMethod.POST, request, new ParameterizedTypeReference<RestIncidentPriority>() {}, incident.getId()).getBody();
            log.debug("Incident Priority for incident '" + ip.incidentId + "': Priority = " + ip.priority + ", Average = " + ip.average);
            incidentPriority = new IncidentPriority();
            incidentPriority.setIncidentId(ip.incidentId);
            incidentPriority.setPriority(new BigDecimal(ip.priority));
            incidentPriority.setAveragePriority(new BigDecimal(ip.average));
            incidentPriority.setIncidents(new BigDecimal(ip.incidents));
        } catch (HttpClientErrorException e) {
            log.error("Http Exception when calling incident priority service - response code : " + e.getRawStatusCode(), e);
            incidentPriority = new IncidentPriority();
            incidentPriority.setIncidentId(incident.getId());
            incidentPriority.setPriority(new BigDecimal(0));
            incidentPriority.setAveragePriority(new BigDecimal(0));
            incidentPriority.setIncidents(new BigDecimal(0));
        }
        Map<String, Object> results = new HashMap<>();
        results.put("IncidentPriority", incidentPriority);
        manager.completeWorkItem(workItem.getId(), results);
    }

    @Override
    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {

    }


    public static class RestIncidentPriority {
        private String incidentId;

        private int priority;

        private double average;

        private int incidents;

        public void setIncidentId(String incidentId) {
            this.incidentId = incidentId;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        public void setAverage(double average) {
            this.average = average;
        }

        public void setIncidents(int incidents) {
            this.incidents = incidents;
        }
    }

}
