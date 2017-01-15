package com.puresoltechnologies.ductiledb.bigtable.cf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStoreConfiguration;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStoreTestUtils;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public class ColumnFamilyEngineCreateAndReopenIT {

    @Test
    public void testCreationAndReopen() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File("/ColumnFamilyEngineCreateAndReopenIT.testCreationAndReopen");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(Key.of("cf"), directory);
	try (ColumnFamilyEngine store = ColumnFamilyEngine.create(storage, columnFamilyDescriptor,
		new LogStoreConfiguration())) {
	    ColumnMap columnMap = new ColumnMap();
	    columnMap.put(Key.of("Column"), ColumnValue.of("Value"));
	    store.put(Key.of("Row"), columnMap);

	}
	try (ColumnFamilyEngine store = ColumnFamilyEngine.reopen(storage, directory)) {
	    assertEquals("Value", Bytes.toString(store.get(Key.of("Row")).get(Key.of("Column")).getBytes()));
	}
    }

    @Test(expected = IOException.class)
    public void testDoubleCreationNotAllowed() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File("/ColumnFamilyEngineCreateAndReopenIT.testDoubleCreationNotAllowed");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(Key.of("cf"), directory);
	try (ColumnFamilyEngine store = ColumnFamilyEngine.create(storage, columnFamilyDescriptor,
		new LogStoreConfiguration())) {
	    ColumnMap columnMap = new ColumnMap();
	    columnMap.put(Key.of("Column"), ColumnValue.of("Value"));
	    store.put(Key.of("Row"), columnMap);
	}
	try (ColumnFamilyEngine store = ColumnFamilyEngine.create(storage, columnFamilyDescriptor,
		new LogStoreConfiguration())) {
	    fail("Should not work.");
	}
    }

}
