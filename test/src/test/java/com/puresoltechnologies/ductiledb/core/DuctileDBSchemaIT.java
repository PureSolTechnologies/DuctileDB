package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema;

public class DuctileDBSchemaIT {

    @Before
    public void removeTables() throws IOException {
	DuctileDBTestHelper.removeTables();
    }

    @Test
    public void testExplicitSchemaCreation() throws IOException {
	try (Connection connection = DuctileDBGraphFactory.createConnection(new BaseConfiguration())) {
	    DuctileDBSchema schema = new DuctileDBSchema(connection);
	    schema.checkAndCreateEnvironment();
	}
    }

    @Test
    public void testImplicitSchemaCreation() throws IOException {
	try (DuctileDBGraph graph = DuctileDBGraphFactory.createGraph(new BaseConfiguration())) {
	    assertNotNull(graph.addVertex());
	}
    }
}
