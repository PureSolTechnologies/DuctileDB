package com.puresoltechnologies.ductiledb.core.schema;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ServiceException;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphFactory;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;

public class HBaseSchemaIT {

    @Before
    public void removeTables() throws IOException, ServiceException {
	DuctileDBTestHelper.removeTables();
    }

    @Test
    public void testExplicitSchemaCreation() throws IOException, ServiceException {
	try (Connection connection = DuctileDBGraphFactory.createConnection(
		AbstractDuctileDBGraphTest.DEFAULT_ZOOKEEPER_HOST, AbstractDuctileDBGraphTest.DEFAULT_ZOOKEEPER_PORT,
		AbstractDuctileDBGraphTest.DEFAULT_MASTER_HOST, AbstractDuctileDBGraphTest.DEFAULT_MASTER_PORT)) {
	    HBaseSchema schema = new HBaseSchema(connection);
	    schema.checkAndCreateEnvironment();
	}
    }

    @Test
    public void testImplicitSchemaCreation() throws IOException, ServiceException {
	try (DuctileDBGraph graph = DuctileDBGraphFactory.createGraph(AbstractDuctileDBGraphTest.DEFAULT_ZOOKEEPER_HOST,
		AbstractDuctileDBGraphTest.DEFAULT_ZOOKEEPER_PORT, AbstractDuctileDBGraphTest.DEFAULT_MASTER_HOST,
		AbstractDuctileDBGraphTest.DEFAULT_MASTER_PORT)) {
	    assertNotNull(graph.addVertex());
	}
    }
}
