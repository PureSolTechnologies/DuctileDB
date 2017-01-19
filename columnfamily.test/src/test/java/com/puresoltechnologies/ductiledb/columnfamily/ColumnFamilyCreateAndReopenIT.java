package com.puresoltechnologies.ductiledb.columnfamily;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStoreConfiguration;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStoreTestUtils;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public class ColumnFamilyCreateAndReopenIT {

    @Test
    public void testCreationAndReopen() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File(
		"/" + ColumnFamilyCreateAndReopenIT.class.getSimpleName() + ".testCreationAndReopen");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(Key.of("cf"), directory);
	try (ColumnFamily store = ColumnFamily.create(storage, columnFamilyDescriptor,
		new LogStoreConfiguration())) {
	    ColumnMap columnMap = new ColumnMap();
	    columnMap.put(Key.of("Column"), ColumnValue.of("Value"));
	    store.put(Key.of("Row"), columnMap);

	}
	try (ColumnFamily store = ColumnFamily.reopen(storage, directory)) {
	    assertEquals("Value", Bytes.toString(store.get(Key.of("Row")).get(Key.of("Column")).getBytes()));
	}
    }

    @Test(expected = IOException.class)
    public void testDoubleCreationNotAllowed() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File(
		"/" + ColumnFamilyCreateAndReopenIT.class.getSimpleName() + ".testDoubleCreationNotAllowed");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(Key.of("cf"), directory);
	try (ColumnFamily store = ColumnFamily.create(storage, columnFamilyDescriptor,
		new LogStoreConfiguration())) {
	    ColumnMap columnMap = new ColumnMap();
	    columnMap.put(Key.of("Column"), ColumnValue.of("Value"));
	    store.put(Key.of("Row"), columnMap);
	}
	try (ColumnFamily store = ColumnFamily.create(storage, columnFamilyDescriptor,
		new LogStoreConfiguration())) {
	    fail("Should not work.");
	}
    }

}
