package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;

public class LogStoreConfigurationTest {

    @Test
    public void testSerialization() throws IOException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

	LogStoreConfiguration configuration = new LogStoreConfiguration();
	configuration.setMaxDataFileSize(12345678l);
	configuration.setMaxCommitLogSize(1234567l);
	configuration.setMaxFileGenerations(5);
	configuration.setBufferSize(123456);

	String jsonString = objectMapper.writeValueAsString(configuration);

	LogStoreConfiguration configuration2 = objectMapper.readValue(jsonString, LogStoreConfiguration.class);

	assertEquals(configuration, configuration2);
    }
}
