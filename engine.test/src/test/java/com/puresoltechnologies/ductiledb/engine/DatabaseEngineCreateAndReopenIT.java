package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStoreTestUtils;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public class DatabaseEngineCreateAndReopenIT {

    @Test
    public void testCreationAndReopen() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File(
		"/" + DatabaseEngineCreateAndReopenIT.class.getSimpleName() + ".testCreationAndReopen");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	try (DatabaseEngineImpl engine = new DatabaseEngineImpl(storage, directory, "storeName",
		new BigTableConfiguration())) {
	    engine.addNamespace("namespace");
	}
	try (DatabaseEngineImpl engine = new DatabaseEngineImpl(storage, directory, "storeName",
		new BigTableConfiguration())) {
	    assertTrue(engine.hasNamespace("namespace"));
	}
    }

    @Test
    public void testCreationAndReopenMultipleNamespaces() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File("/" + DatabaseEngineCreateAndReopenIT.class.getSimpleName()
		+ ".testCreationAndReopenMultipleColumnFamilies");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	try (DatabaseEngineImpl engine = new DatabaseEngineImpl(storage, directory, "storeName",
		new BigTableConfiguration())) {
	    engine.addNamespace("namespace");
	    engine.addNamespace("namespace2");
	    engine.addNamespace("namespace3");
	}
	try (DatabaseEngineImpl engine = new DatabaseEngineImpl(storage, directory, "storeName",
		new BigTableConfiguration())) {
	    assertTrue(engine.hasNamespace("namespace"));
	    assertTrue(engine.hasNamespace("namespace2"));
	    assertTrue(engine.hasNamespace("namespace3"));
	}
    }

}
