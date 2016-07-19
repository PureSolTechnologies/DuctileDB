package com.puresoltechnologies.ductiledb.core.graph.schema;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBFactory;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class HBaseSchemaIT {

    @Before
    public void removeTables() throws StorageException, IOException, SchemaException {
	DuctileDBTestHelper.removeTables();
    }

    @Test
    public void testExplicitSchemaCreation() throws IOException, StorageException, SchemaException {
	try (DuctileDB ductileDB = AbstractDuctileDBTest.createDuctileDB();
		DatabaseEngine storageEngine = ((DuctileDBGraphImpl) ductileDB.getGraph()).getStorageEngine()) {
	    GraphSchema schema = new GraphSchema(storageEngine);
	    schema.checkAndCreateEnvironment();
	}
    }

    @Test
    public void testImplicitSchemaCreation() throws IOException, StorageException, SchemaException {
	try (DuctileDB ductileDB = DuctileDBFactory.connect(AbstractDuctileDBTest.readTestConfigration())) {
	    DuctileDBGraph graph = ductileDB.getGraph();
	    assertNotNull(graph.addVertex());
	}
    }
}
