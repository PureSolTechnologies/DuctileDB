package com.puresoltechnologies.ductiledb.bigtable;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;

public class BigTableConfigurationTest {

    @Test
    public void testSerialization() throws IOException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

	BigTableConfiguration configuration = new BigTableConfiguration();

	String jsonString = objectMapper.writeValueAsString(configuration);

	BigTableConfiguration configuration2 = objectMapper.readValue(jsonString, BigTableConfiguration.class);

	assertEquals(configuration, configuration2);
    }
}
