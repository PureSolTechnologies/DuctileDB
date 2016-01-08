package com.puresoltechnologies.ductiledb.core.schema;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphFactory;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.core.schema.HBaseSchema;

public class HBaseSchemaIT {

    @Before
    public void removeTables() throws IOException {
	DuctileDBTestHelper.removeTables();
    }

    @Test
    public void testExplicitSchemaCreation() throws IOException {
	try (Connection connection = DuctileDBGraphFactory.createConnection(new BaseConfiguration())) {
	    HBaseSchema schema = new HBaseSchema(connection);
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
