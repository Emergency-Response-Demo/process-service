package com.redhat.cajun.navy.process.wih;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.redhat.cajun.navy.process.message.model.CloudEventBuilder;
import com.redhat.cajun.navy.process.message.model.CreateMissionCommand;
import com.redhat.cajun.navy.process.message.model.IncidentAssignmentEvent;
import com.redhat.cajun.navy.process.message.model.Responder;
import com.redhat.cajun.navy.process.message.model.SetResponderUnavailableCommand;
import com.redhat.cajun.navy.process.message.model.UpdateIncidentCommand;
import com.redhat.cajun.navy.process.message.model.UpdateResponderCommand;
import com.redhat.cajun.navy.process.outbox.OutboxEventEmitter;
import com.redhat.cajun.navy.rules.model.Incident;
import com.redhat.cajun.navy.rules.model.Mission;
import com.redhat.cajun.navy.rules.model.Status;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.data.PojoCloudEventData;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.kafka.core.KafkaTemplate;


public class KafkaMessageSenderWorkItemHandlerTest {

    @Mock
    private KafkaTemplate<String, CloudEvent> kafkaTemplate;

    @Mock
    private WorkItem workItem;

    @Mock
    private WorkItemManager workItemManager;

    @Mock
    private OutboxEventEmitter outboxEventEmitter;

    @Captor
    private ArgumentCaptor<CloudEvent> cloudEventCaptor;

    private KafkaMessageSenderWorkItemHandler wih;

    @Before
    public void setup() {
        initMocks(this);
        wih = new KafkaMessageSenderWorkItemHandler();
        setField(wih, "createMissionCommandDestination", "topic-mission-command", String.class);
        setField(wih, "updateResponderCommandDestination", "topic-responder-command", String.class);
        setField(wih, "setResponderUnavailableCommandDestination", "topic-responder-command", String.class);
        setField(wih, "updateIncidentCommandDestination", "topic-incident-command", String.class);
        setField(wih, "incidentAssignmentEventDestination", "topic-incident-event", String.class);
        setField(wih, null, outboxEventEmitter, OutboxEventEmitter.class);
        wih.init();
    }

    @Test
    public void testExecuteWorkItem() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "testPayloadType");
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.addPayloadBuilder("testPayloadType", "testMessageType", "topic-test", TestMessageEvent::build);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(any(CloudEvent.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateMissionCommandMessageType() {

        Mission mission = new Mission();
        mission.setIncidentId("incident123");
        mission.setIncidentLat(new BigDecimal("30.12345"));
        mission.setIncidentLong(new BigDecimal("-70.98765"));
        mission.setResponderId("responder123");
        mission.setResponderStartLat(new BigDecimal("40.12345"));
        mission.setResponderStartLong(new BigDecimal("-80.98765"));
        mission.setDestinationLat(new BigDecimal("50.12345"));
        mission.setDestinationLong(new BigDecimal("-90.98765"));

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "CreateMission");
        parameters.put("Payload", mission);
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(cloudEventCaptor.capture());

        CloudEvent cloudEvent = cloudEventCaptor.getValue();
        assertThat(cloudEvent.getType(), equalTo("CreateMissionCommand"));
        assertThat(cloudEvent.getSource().toString(), equalTo("emergency-response/process-service"));
        assertThat(cloudEvent.getSpecVersion().toString(), equalTo("1.0"));
        assertThat(cloudEvent.getTime(), notNullValue());
        assertThat(cloudEvent.getExtensionNames().size(), equalTo(2));
        assertThat(cloudEvent.getExtension("aggregatetype"), equalTo("topic-mission-command"));
        assertThat(cloudEvent.getExtension("aggregateid"), equalTo("incident123"));
        assertThat(cloudEvent.getData(), notNullValue());
        assertThat(cloudEvent.getData(), is(instanceOf(PojoCloudEventData.class)));
        PojoCloudEventData<CreateMissionCommand> cloudEventData = (PojoCloudEventData<CreateMissionCommand>) cloudEvent.getData();
        CreateMissionCommand cmd = cloudEventData.getValue();
        assertThat(cmd.getIncidentId(), equalTo("incident123"));
        assertThat(cmd.getIncidentLat(), equalTo(new BigDecimal("30.12345")));
        assertThat(cmd.getIncidentLong(), equalTo(new BigDecimal("-70.98765")));
        assertThat(cmd.getResponderId(), equalTo("responder123"));
        assertThat(cmd.getResponderStartLat(), equalTo(new BigDecimal("40.12345")));
        assertThat(cmd.getResponderStartLong(), equalTo(new BigDecimal("-80.98765")));
        assertThat(cmd.getDestinationLat(), equalTo(new BigDecimal("50.12345")));
        assertThat(cmd.getDestinationLong(), equalTo(new BigDecimal("-90.98765")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSetResponderUnavailableMessageType() {
        Mission mission = new Mission();
        mission.setIncidentId("incident123");
        mission.setIncidentLat(new BigDecimal("30.12345"));
        mission.setIncidentLong(new BigDecimal("-70.98765"));
        mission.setResponderId("responder123");
        mission.setResponderStartLat(new BigDecimal("40.12345"));
        mission.setResponderStartLong(new BigDecimal("-80.98765"));
        mission.setDestinationLat(new BigDecimal("50.12345"));
        mission.setDestinationLong(new BigDecimal("-90.98765"));

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "SetResponderUnavailable");
        parameters.put("Payload", mission);
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(cloudEventCaptor.capture());

        CloudEvent cloudEvent = cloudEventCaptor.getValue();
        assertThat(cloudEvent.getType(), equalTo("SetResponderUnavailableCommand"));
        assertThat(cloudEvent.getSource().toString(), equalTo("emergency-response/process-service"));
        assertThat(cloudEvent.getSpecVersion().toString(), equalTo("1.0"));
        assertThat(cloudEvent.getTime(), notNullValue());
        assertThat(cloudEvent.getExtensionNames().size(), equalTo(3));
        assertThat(cloudEvent.getData(), notNullValue());
        assertThat(cloudEvent.getExtension("incidentid"), equalTo("incident123"));
        assertThat(cloudEvent.getExtension("aggregatetype"), equalTo("topic-responder-command"));
        assertThat(cloudEvent.getExtension("aggregateid"), equalTo("responder123"));
        assertThat(cloudEvent.getData(), is(instanceOf(PojoCloudEventData.class)));
        PojoCloudEventData<SetResponderUnavailableCommand> cloudEventData = (PojoCloudEventData<SetResponderUnavailableCommand>) cloudEvent.getData();
        SetResponderUnavailableCommand command = cloudEventData.getValue();
        assertThat(command, notNullValue());
        Responder responder = command.getResponder();
        assertThat(responder, notNullValue());
        assertThat(responder.getId(), equalTo("responder123"));
        assertThat(responder.isAvailable(), equalTo(false));
        assertThat(responder.getName(), nullValue());
        assertThat(responder.getPhoneNumber(), nullValue());
        assertThat(responder.getLatitude(), nullValue());
        assertThat(responder.getLongitude(), nullValue());
        assertThat(responder.getBoatCapacity(), nullValue());
        assertThat(responder.isMedicalKit(), nullValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateIncidentMessageTypeStatusAssigned() {
        Incident incident = new Incident();
        incident.setId("incident123");
        incident.setStatus("Assigned");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "UpdateIncident");
        parameters.put("Payload", incident);
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(cloudEventCaptor.capture());

        CloudEvent cloudEvent = cloudEventCaptor.getValue();
        assertThat(cloudEvent.getType(), equalTo("UpdateIncidentCommand"));
        assertThat(cloudEvent.getSource().toString(), equalTo("emergency-response/process-service"));
        assertThat(cloudEvent.getSpecVersion().toString(), equalTo("1.0"));
        assertThat(cloudEvent.getTime(), notNullValue());
        assertThat(cloudEvent.getExtensionNames().size(), equalTo(2));
        assertThat(cloudEvent.getExtension("aggregatetype"), equalTo("topic-incident-command"));
        assertThat(cloudEvent.getExtension("aggregateid"), equalTo("incident123"));
        assertThat(cloudEvent.getData(), notNullValue());
        assertThat(cloudEvent.getData(), is(instanceOf(PojoCloudEventData.class)));
        PojoCloudEventData<UpdateIncidentCommand> cloudEventData = (PojoCloudEventData<UpdateIncidentCommand>) cloudEvent.getData();
        UpdateIncidentCommand command = cloudEventData.getValue();
        assertThat(command, notNullValue());
        com.redhat.cajun.navy.process.message.model.Incident toUpdate = command.getIncident();
        assertThat(toUpdate, notNullValue());
        assertThat(toUpdate.getId(), equalTo("incident123"));
        assertThat(toUpdate.getStatus(), equalTo("ASSIGNED"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateIncidentMessageTypeStatusPickedUp() {
        Incident incident = new Incident();
        incident.setId("incident123");
        incident.setStatus("PickedUp");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "UpdateIncident");
        parameters.put("Payload", incident);
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(cloudEventCaptor.capture());

        CloudEvent cloudEvent = cloudEventCaptor.getValue();
        assertThat(cloudEvent.getType(), equalTo("UpdateIncidentCommand"));
        assertThat(cloudEvent.getSource().toString(), equalTo("emergency-response/process-service"));
        assertThat(cloudEvent.getSpecVersion().toString(), equalTo("1.0"));
        assertThat(cloudEvent.getTime(), notNullValue());
        assertThat(cloudEvent.getExtensionNames().size(), equalTo(2));
        assertThat(cloudEvent.getExtension("aggregatetype"), equalTo("topic-incident-command"));
        assertThat(cloudEvent.getExtension("aggregateid"), equalTo("incident123"));
        assertThat(cloudEvent.getData(), notNullValue());
        assertThat(cloudEvent.getData(), is(instanceOf(PojoCloudEventData.class)));
        PojoCloudEventData<UpdateIncidentCommand> cloudEventData = (PojoCloudEventData<UpdateIncidentCommand>) cloudEvent.getData();
        UpdateIncidentCommand command = cloudEventData.getValue();
        assertThat(command, notNullValue());
        com.redhat.cajun.navy.process.message.model.Incident toUpdate = command.getIncident();
        assertThat(toUpdate, notNullValue());
        assertThat(toUpdate.getId(), equalTo("incident123"));
        assertThat(toUpdate.getStatus(), equalTo("PICKEDUP"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateIncidentMessageTypeStatusDelivered() {
        Incident incident = new Incident();
        incident.setId("incident123");
        incident.setStatus("Delivered");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "UpdateIncident");
        parameters.put("Payload", incident);
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(cloudEventCaptor.capture());

        CloudEvent cloudEvent = cloudEventCaptor.getValue();
        assertThat(cloudEvent.getType(), equalTo("UpdateIncidentCommand"));
        assertThat(cloudEvent.getSource().toString(), equalTo("emergency-response/process-service"));
        assertThat(cloudEvent.getSpecVersion().toString(), equalTo("1.0"));
        assertThat(cloudEvent.getTime(), notNullValue());
        assertThat(cloudEvent.getExtensionNames().size(), equalTo(2));
        assertThat(cloudEvent.getExtension("aggregatetype"), equalTo("topic-incident-command"));
        assertThat(cloudEvent.getExtension("aggregateid"), equalTo("incident123"));
        assertThat(cloudEvent.getData(), notNullValue());
        assertThat(cloudEvent.getData(), is(instanceOf(PojoCloudEventData.class)));
        PojoCloudEventData<UpdateIncidentCommand> cloudEventData = (PojoCloudEventData<UpdateIncidentCommand>) cloudEvent.getData();
        UpdateIncidentCommand command = cloudEventData.getValue();
        assertThat(command, notNullValue());
        com.redhat.cajun.navy.process.message.model.Incident toUpdate = command.getIncident();
        assertThat(toUpdate, notNullValue());
        assertThat(toUpdate.getId(), equalTo("incident123"));
        assertThat(toUpdate.getStatus(), equalTo("RESCUED"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIncidentAssignmentEventMessageTypeStatusAssigned() {
        Mission mission = new Mission();
        mission.setIncidentId("incident123");
        mission.setIncidentLat(new BigDecimal("30.12345"));
        mission.setIncidentLong(new BigDecimal("-70.98765"));
        mission.setResponderId("responder123");
        mission.setResponderStartLat(new BigDecimal("40.12345"));
        mission.setResponderStartLong(new BigDecimal("-80.98765"));
        mission.setDestinationLat(new BigDecimal("50.12345"));
        mission.setDestinationLong(new BigDecimal("-90.98765"));
        mission.setStatus(Status.ASSIGNED);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "IncidentAssignment");
        parameters.put("Payload", mission);
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(cloudEventCaptor.capture());

        CloudEvent cloudEvent = cloudEventCaptor.getValue();
        assertThat(cloudEvent.getType(), equalTo("IncidentAssignmentEvent"));
        assertThat(cloudEvent.getSource().toString(), equalTo("emergency-response/process-service"));
        assertThat(cloudEvent.getSpecVersion().toString(), equalTo("1.0"));
        assertThat(cloudEvent.getTime(), notNullValue());
        assertThat(cloudEvent.getExtensionNames().size(), equalTo(2));
        assertThat(cloudEvent.getExtension("aggregatetype"), equalTo("topic-incident-event"));
        assertThat(cloudEvent.getExtension("aggregateid"), equalTo("incident123"));
        assertThat(cloudEvent.getData(), notNullValue());
        assertThat(cloudEvent.getData(), is(instanceOf(PojoCloudEventData.class)));
        PojoCloudEventData<IncidentAssignmentEvent> cloudEventData = (PojoCloudEventData<IncidentAssignmentEvent>) cloudEvent.getData();
        IncidentAssignmentEvent event = cloudEventData.getValue();
        assertThat(event, notNullValue());
        assertThat(event.getIncidentId(), equalTo("incident123"));
        assertThat(event.getAssignment(), equalTo(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIncidentAssignmentEventMessageTypeStatusUnassigned() {
        Mission mission = new Mission();
        mission.setIncidentId("incident123");
        mission.setIncidentLat(new BigDecimal("30.12345"));
        mission.setIncidentLong(new BigDecimal("-70.98765"));
        mission.setResponderId("responder123");
        mission.setResponderStartLat(new BigDecimal("40.12345"));
        mission.setResponderStartLong(new BigDecimal("-80.98765"));
        mission.setDestinationLat(new BigDecimal("50.12345"));
        mission.setDestinationLong(new BigDecimal("-90.98765"));
        mission.setStatus(Status.UNASSIGNED);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "IncidentAssignment");
        parameters.put("Payload", mission);
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(cloudEventCaptor.capture());

        CloudEvent cloudEvent = cloudEventCaptor.getValue();
        assertThat(cloudEvent.getType(), equalTo("IncidentAssignmentEvent"));
        assertThat(cloudEvent.getSource().toString(), equalTo("emergency-response/process-service"));
        assertThat(cloudEvent.getSpecVersion().toString(), equalTo("1.0"));
        assertThat(cloudEvent.getTime(), notNullValue());
        assertThat(cloudEvent.getExtensionNames().size(), equalTo(2));
        assertThat(cloudEvent.getExtension("aggregatetype"), equalTo("topic-incident-event"));
        assertThat(cloudEvent.getExtension("aggregateid"), equalTo("incident123"));
        assertThat(cloudEvent.getData(), notNullValue());
        assertThat(cloudEvent.getData(), is(instanceOf(PojoCloudEventData.class)));
        PojoCloudEventData<IncidentAssignmentEvent> cloudEventData = (PojoCloudEventData<IncidentAssignmentEvent>) cloudEvent.getData();
        IncidentAssignmentEvent event = cloudEventData.getValue();
        assertThat(event, notNullValue());
        assertThat(event.getIncidentId(), equalTo("incident123"));
        assertThat(event.getAssignment(), equalTo(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateResponderMessageType() {
        Mission mission = new Mission();
        mission.setIncidentId("incident123");
        mission.setIncidentLat(new BigDecimal("30.12345"));
        mission.setIncidentLong(new BigDecimal("-70.98765"));
        mission.setResponderId("responder123");
        mission.setResponderStartLat(new BigDecimal("40.12345"));
        mission.setResponderStartLong(new BigDecimal("-80.98765"));
        mission.setDestinationLat(new BigDecimal("50.12345"));
        mission.setDestinationLong(new BigDecimal("-90.98765"));

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("MessageType", "UpdateResponder");
        parameters.put("Payload", mission);
        when(workItem.getParameters()).thenReturn(parameters);
        when(workItem.getId()).thenReturn(1L);

        wih.executeWorkItem(workItem, workItemManager);
        verify(workItemManager).completeWorkItem(eq(1L), anyMap());
        verify(outboxEventEmitter).emitCloudEvent(cloudEventCaptor.capture());

        CloudEvent cloudEvent = cloudEventCaptor.getValue();
        assertThat(cloudEvent.getType(), equalTo("UpdateResponderCommand"));
        assertThat(cloudEvent.getSource().toString(), equalTo("emergency-response/process-service"));
        assertThat(cloudEvent.getSpecVersion().toString(), equalTo("1.0"));
        assertThat(cloudEvent.getTime(), notNullValue());
        assertThat(cloudEvent.getExtensionNames().size(), equalTo(2));
        assertThat(cloudEvent.getExtension("incidentid"), nullValue());
        assertThat(cloudEvent.getExtension("aggregatetype"), equalTo("topic-responder-command"));
        assertThat(cloudEvent.getExtension("aggregateid"), equalTo("responder123"));
        assertThat(cloudEvent.getData(), notNullValue());
        assertThat(cloudEvent.getData(), is(instanceOf(PojoCloudEventData.class)));
        PojoCloudEventData<UpdateResponderCommand> cloudEventData = (PojoCloudEventData<UpdateResponderCommand>) cloudEvent.getData();
        UpdateResponderCommand command = cloudEventData.getValue();
        assertThat(command, notNullValue());
        Responder responder = command.getResponder();
        assertThat(responder, notNullValue());
        assertThat(responder.getId(), equalTo("responder123"));
        assertThat(responder.isAvailable(), equalTo(true));
        assertThat(responder.getName(), nullValue());
        assertThat(responder.getPhoneNumber(), nullValue());
        assertThat(responder.getLatitude(), equalTo(new BigDecimal("50.12345")));
        assertThat(responder.getLongitude(), equalTo(new BigDecimal("-90.98765")));
        assertThat(responder.getBoatCapacity(), nullValue());
        assertThat(responder.isMedicalKit(), nullValue());
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class TestMessageEvent {

        static CloudEvent build(Pair<String, String> messageTypeAndDestination, Map<String, Object> parameters) {
            TestMessageEvent event = new TestMessageEvent();
            return new CloudEventBuilder<TestMessageEvent>()
                    .withType(messageTypeAndDestination.getLeft())
                    .withData(event)
                    .withExtension("aggregatetype", messageTypeAndDestination.getRight())
                    .withExtension("aggregateid", "testKey")
                    .build();
        }

    }
}
