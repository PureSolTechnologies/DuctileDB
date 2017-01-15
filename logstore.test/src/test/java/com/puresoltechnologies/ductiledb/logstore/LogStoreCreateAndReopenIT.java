package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public class LogStoreCreateAndReopenIT {

    @Test
    public void testCreationAndReopen() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File("LogStoreCreateAndReopenIT.testCreationAndReopen");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	try (LogStructuredStore store = LogStructuredStore.create(storage, directory, new LogStoreConfiguration())) {
	    store.put(Key.of("Key"), Bytes.toBytes("Value"));
	}
	try (LogStructuredStore store = LogStructuredStore.reopen(storage, directory)) {
	    assertEquals("Value", Bytes.toString(store.get(Key.of("Key"))));
	}
    }

    @Test(expected = IOException.class)
    public void testDoubleCreationNotAllowed() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File("LogStoreCreateAndReopenIT.testDoubleCreationNotAllowed");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	try (LogStructuredStore store = LogStructuredStore.create(storage, directory, new LogStoreConfiguration())) {
	    store.put(Key.of("Key"), Bytes.toBytes("Value"));
	}
	try (LogStructuredStore store = LogStructuredStore.create(storage, directory, new LogStoreConfiguration())) {
	    fail("Should not work.");
	}
    }

}
