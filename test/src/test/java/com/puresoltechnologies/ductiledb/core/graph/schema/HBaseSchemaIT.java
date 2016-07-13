package com.puresoltechnologies.ductiledb.core.graph.schema;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ServiceException;
import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBFactory;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBTestHelper;

public class HBaseSchemaIT {

    @Before
    public void removeTables() throws IOException, ServiceException {
	DuctileDBTestHelper.removeTables();
    }

    @Test
    public void testExplicitSchemaCreation() throws IOException, ServiceException {
	try (DuctileDB ductileDB = AbstractDuctileDBTest.createDuctileDB();
		Connection connection = ((DuctileDBGraphImpl) ductileDB.getGraph()).getConnection()) {
	    HBaseSchema schema = new HBaseSchema(connection);
	    schema.checkAndCreateEnvironment();
	}
    }

    @Test
    public void testImplicitSchemaCreation() throws IOException, ServiceException {
	try (DuctileDB ductileDB = DuctileDBFactory.connect(AbstractDuctileDBTest.hadoopHome,
		AbstractDuctileDBTest.hbaseHome)) {
	    DuctileDBGraph graph = ductileDB.getGraph();
	    assertNotNull(graph.addVertex());
	}
    }
}
