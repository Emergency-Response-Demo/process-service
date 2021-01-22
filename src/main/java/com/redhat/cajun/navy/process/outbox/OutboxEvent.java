package com.redhat.cajun.navy.process.outbox;

import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

import org.hibernate.annotations.Type;

@Entity
@Table(name = "process_service_outbox")
public class OutboxEvent {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "aggregatetype")
    @NotNull
    private String aggregateType;

    @Column(name = "aggregateid")
    @NotNull
    private String aggregateId;

    @NotNull
    private String type;

    @Lob
    @Type(type = "org.hibernate.type.TextType")
    @NotNull
    private String payload;

    @Column(name="ce_specversion")
    @NotNull
    private String ceSpecVersion;

    @Column(name="ce_source")
    @NotNull
    private String ceSource;

    @Column(name="ce_time")
    @NotNull
    private String ceTime;

    @Column(name="ce_datacontenttype")
    @NotNull
    private String ceDataContentType;

    @Column(name="ce_incidentid")
    private String ceIncidentId;

    OutboxEvent() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getCeSpecVersion() {
        return ceSpecVersion;
    }

    public void setCeSpecVersion(String ceSpecVersion) {
        this.ceSpecVersion = ceSpecVersion;
    }

    public String getCeSource() {
        return ceSource;
    }

    public void setCeSource(String ceSource) {
        this.ceSource = ceSource;
    }

    public String getCeTime() {
        return ceTime;
    }

    public void setCeTime(String ceTime) {
        this.ceTime = ceTime;
    }

    public String getCeDataContentType() {
        return ceDataContentType;
    }

    public void setCeDataContentType(String ceDataContentType) {
        this.ceDataContentType = ceDataContentType;
    }

    public String getCeIncidentId() {
        return ceIncidentId;
    }

    public void setCeIncidentId(String ceIncidentId) {
        this.ceIncidentId = ceIncidentId;
    }
}
