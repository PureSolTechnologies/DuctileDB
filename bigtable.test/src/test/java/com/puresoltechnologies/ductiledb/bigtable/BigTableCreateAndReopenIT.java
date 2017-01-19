package com.puresoltechnologies.ductiledb.bigtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStoreTestUtils;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public class BigTableCreateAndReopenIT {

    @Test
    public void testCreationAndReopen() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File("/" + BigTableCreateAndReopenIT.class.getSimpleName() + ".testCreationAndReopen");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	TableDescriptor tableDescriptor = new TableDescriptor("table", "description", directory);
	try (BigTable store = BigTable.create(storage, tableDescriptor, new BigTableConfiguration())) {
	    store.addColumnFamily(Key.of("ColumnFamily"));
	    Put put = new Put(Key.of("Row"));
	    put.addColumn(Key.of("ColumnFamily"), Key.of("Column"), ColumnValue.of("Value"));
	    store.put(put);

	}
	try (BigTable store = BigTable.reopen(storage, directory)) {
	    Get get = new Get(Key.of("Row"));
	    get.addColumn(Key.of("ColumnFamily"), Key.of("Column"));
	    Result result = store.get(get);
	    assertEquals("Value",
		    Bytes.toString(result.getFamilyMap(Key.of("ColumnFamily")).get(Key.of("Column")).getBytes()));
	}
    }

    @Test
    public void testCreationAndReopenMultipleColumnFamilies() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File(
		"/" + BigTableCreateAndReopenIT.class.getSimpleName() + ".testCreationAndReopenMultipleColumnFamilies");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	TableDescriptor tableDescriptor = new TableDescriptor("table", "description", directory);
	try (BigTable store = BigTable.create(storage, tableDescriptor, new BigTableConfiguration())) {
	    store.addColumnFamily(Key.of("ColumnFamily"));
	    store.addColumnFamily(Key.of("ColumnFamily2"));
	    store.addColumnFamily(Key.of("ColumnFamily3"));
	    Put put = new Put(Key.of("Row"));
	    put.addColumn(Key.of("ColumnFamily"), Key.of("Column"), ColumnValue.of("Value"));
	    store.put(put);
	    put = new Put(Key.of("Row2"));
	    put.addColumn(Key.of("ColumnFamily2"), Key.of("Column2"), ColumnValue.of("Value2"));
	    store.put(put);
	    put = new Put(Key.of("Row3"));
	    put.addColumn(Key.of("ColumnFamily3"), Key.of("Column3"), ColumnValue.of("Value3"));
	    store.put(put);

	}
	try (BigTable store = BigTable.reopen(storage, directory)) {
	    Get get = new Get(Key.of("Row"));
	    get.addColumn(Key.of("ColumnFamily"), Key.of("Column"));
	    Result result = store.get(get);
	    assertEquals("Value",
		    Bytes.toString(result.getFamilyMap(Key.of("ColumnFamily")).get(Key.of("Column")).getBytes()));

	    get = new Get(Key.of("Row2"));
	    get.addColumn(Key.of("ColumnFamily2"), Key.of("Column2"));
	    result = store.get(get);
	    assertEquals("Value2",
		    Bytes.toString(result.getFamilyMap(Key.of("ColumnFamily2")).get(Key.of("Column2")).getBytes()));

	    get = new Get(Key.of("Row3"));
	    get.addColumn(Key.of("ColumnFamily3"), Key.of("Column3"));
	    result = store.get(get);
	    assertEquals("Value3",
		    Bytes.toString(result.getFamilyMap(Key.of("ColumnFamily3")).get(Key.of("Column3")).getBytes()));
	}
    }

    @Test(expected = IOException.class)
    public void testDoubleCreationNotAllowed() throws IOException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	Storage storage = StorageFactory.getStorageInstance(configuration);

	File directory = new File(
		"/" + BigTableCreateAndReopenIT.class.getSimpleName() + ".testDoubleCreationNotAllowed");
	if (storage.exists(directory)) {
	    storage.removeDirectory(directory, true);
	}
	TableDescriptor tableDescriptor = new TableDescriptor("table", "description", directory);
	try (BigTable store = BigTable.create(storage, tableDescriptor, new BigTableConfiguration())) {
	    store.addColumnFamily(Key.of("ColumnFamily"));
	    Put put = new Put(Key.of("Row"));
	    put.addColumn(Key.of("ColumnFamily"), Key.of("Column"), ColumnValue.of("Value"));
	    store.put(put);
	}
	try (BigTable store = BigTable.create(storage, tableDescriptor, new BigTableConfiguration())) {
	    fail("Should not work.");
	}
    }

}
