package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;

public class KeyTest {

    @Test
    public void testSerialization() throws IOException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();

	Key key = Key.of("KeyName");
	String jsonString = objectMapper.writeValueAsString(key);
	Key key2 = objectMapper.readValue(jsonString, Key.class);
	assertEquals(key, key2);
    }

}
