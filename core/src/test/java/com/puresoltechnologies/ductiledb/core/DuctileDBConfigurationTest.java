package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreConfiguration;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphConfiguration;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;

public class DuctileDBConfigurationTest {

    @Test
    public void testSerialization() throws IOException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	DuctileDBConfiguration configuration = new DuctileDBConfiguration();
	configuration.setBigTableEngine(new BigTableConfiguration());
	configuration.setBlobStore(new BlobStoreConfiguration());
	configuration.setGraph(new DuctileDBGraphConfiguration());

	String jsonString = objectMapper.writeValueAsString(configuration);
	DuctileDBConfiguration configuration2 = objectMapper.readValue(jsonString, DuctileDBConfiguration.class);
	assertEquals(configuration, configuration2);
    }

}
