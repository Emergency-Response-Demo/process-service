package com.redhat.cajun.navy.process.message.model;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class MessageTest {

    @Test
    public void testHeaderIsIgnoredWhenNull() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        Message<UpdateResponderCommand> message =
                new Message.Builder<>("testType", "testInvoker",
                        new UpdateResponderCommand.Builder(new Responder.Builder("responder1234").build()).build()).build();

        String messageAsJson = mapper.writeValueAsString(message);

        System.out.println(messageAsJson);

        assertThat(messageAsJson, containsString("testType"));
        assertThat(messageAsJson, not(containsString("header")));
    }

}
