package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStoreTestUtils;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public class NamespaceCreateAndReopenIT {

    @Test
    public void testCreationAndReopen() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File("/" + NamespaceCreateAndReopenIT.class.getSimpleName() + ".testCreationAndReopen");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	NamespaceDescriptor namespaceDescriptor = new NamespaceDescriptor(directory);
	try (Namespace namespace = Namespace.create(storage, namespaceDescriptor, new BigTableConfiguration())) {
	    namespace.addTable("table", "description");

	}
	try (Namespace namespace = Namespace.reopen(storage, directory)) {
	    assertTrue(namespace.hasTable("table"));
	}
    }

    @Test
    public void testCreationAndReopenMultipleTables() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File("/" + NamespaceCreateAndReopenIT.class.getSimpleName()
		+ ".testCreationAndReopenMultipleColumnFamilies");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	NamespaceDescriptor namespaceDescriptor = new NamespaceDescriptor(directory);
	try (Namespace namespace = Namespace.create(storage, namespaceDescriptor, new BigTableConfiguration())) {
	    namespace.addTable("table", "description");
	    namespace.addTable("table2", "description2");
	    namespace.addTable("table3", "description3");
	}
	try (Namespace namespace = Namespace.reopen(storage, directory)) {
	    assertTrue(namespace.hasTable("table"));
	}
    }

    @Test(expected = IOException.class)
    public void testDoubleCreationNotAllowed() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File(
		"/" + NamespaceCreateAndReopenIT.class.getSimpleName() + ".testDoubleCreationNotAllowed");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	NamespaceDescriptor namespaceDescriptor = new NamespaceDescriptor(directory);
	try (Namespace namespace = Namespace.create(storage, namespaceDescriptor, new BigTableConfiguration())) {
	    namespace.addTable("table", "description");
	}
	try (Namespace namespace = Namespace.create(storage, namespaceDescriptor, new BigTableConfiguration())) {
	    fail("Should not work.");
	}
    }

}
