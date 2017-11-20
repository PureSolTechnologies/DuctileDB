package com.puresoltechnologies.ductiledb.engine.cf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyImpl;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.engine.io.DataFilenameFilter;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;
import com.puresoltechnologies.ductiledb.logstore.Row;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.logstore.index.io.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.logstore.io.DataFileReader;
import com.puresoltechnologies.ductiledb.logstore.io.DataFileSet;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class ColumnFamilyEngineIT extends AbstractColumnFamiliyEngineTest {

    private static String NAMESPACE = ColumnFamilyEngineIT.class.getSimpleName();

    @Test
    public void testMemtableCRUD() throws IOException {
	try (ColumnFamilyImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testMemtableCRUD", "testcf")) {
	    // Check behavior of empty Memtable
	    ColumnMap entry = columnFamilyEngine.get(Key.of(0l));
	    assertTrue(entry.isEmpty());
	    columnFamilyEngine.delete(Key.of(0l));
	    // Check put
	    ColumnMap columns = new ColumnMap();
	    columns.put(Key.of(123l), ColumnValue.of(123l));
	    columnFamilyEngine.put(Key.of(0l), columns);
	    entry = columnFamilyEngine.get(Key.of(0l));
	    assertNotNull(entry);
	    assertEquals(columns, entry);
	    // Check put of new value does not change former
	    ColumnMap columns2 = new ColumnMap();
	    columns2.put(Key.of(1234l), ColumnValue.of(1234l));
	    columnFamilyEngine.put(Key.of(1l), columns2);
	    entry = columnFamilyEngine.get(Key.of(0l));
	    assertNotNull(entry);
	    assertEquals(columns, entry);
	    entry = columnFamilyEngine.get(Key.of(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);
	    // Check put
	    ColumnMap columns3 = new ColumnMap();
	    columns3.put(Key.of(12345l), ColumnValue.of(12345l));
	    columnFamilyEngine.put(Key.of(0l), columns3);
	    entry = columnFamilyEngine.get(Key.of(0l));
	    assertNotNull(entry);
	    columns3.put(Key.of(123l), ColumnValue.of(123l));
	    assertEquals(columns3, entry);
	    entry = columnFamilyEngine.get(Key.of(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);
	    // Check put
	    ColumnMap columns4 = new ColumnMap();
	    columns4.put(Key.of(12345l), ColumnValue.of(123456l));
	    columnFamilyEngine.put(Key.of(0l), columns4);
	    entry = columnFamilyEngine.get(Key.of(0l));
	    assertNotNull(entry);
	    columns4.put(Key.of(123l), ColumnValue.of(123l));
	    assertEquals(columns4, entry);
	    entry = columnFamilyEngine.get(Key.of(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);
	    // Check delete
	    HashSet<Key> columnsToDelete = new HashSet<>();
	    columnsToDelete.add(Key.of(123l));
	    columnFamilyEngine.delete(Key.of(0l), columnsToDelete);
	    entry = columnFamilyEngine.get(Key.of(0l));
	    assertNotNull(entry);
	    ColumnMap columns5 = new ColumnMap();
	    columns5.put(Key.of(12345l), ColumnValue.of(123456l));
	    assertEquals(columns5, entry);
	    entry = columnFamilyEngine.get(Key.of(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);
	    // Check delete
	    columnFamilyEngine.delete(Key.of(0l));
	    entry = columnFamilyEngine.get(Key.of(0l));
	    assertTrue(entry.isEmpty());
	    entry = columnFamilyEngine.get(Key.of(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);

	}
    }

    @Test
    public void testWideRow() throws IOException {
	try (ColumnFamilyImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testWideRow", "testcf")) {
	    ColumnMap entry = columnFamilyEngine.get(Key.of(12345l));
	    assertTrue(entry.isEmpty());
	    columnFamilyEngine.delete(Key.of(12345l));

	    ColumnMap columnMap = new ColumnMap();
	    for (int i = 1; i <= 100000; i++) {
		columnMap.put(Key.of((long) i), ColumnValue.of((long) i));
	    }
	    columnFamilyEngine.put(Key.of(12345l), columnMap);

	    // Check put
	    entry = columnFamilyEngine.get(Key.of(12345l));
	    assertNotNull(entry);
	    assertEquals(columnMap, entry);
	}
    }

    /**
     * This test checks for a small amount of data in memory functionality like put,
     * get, re-put and delete.
     * 
     * @throws SchemaException
     */
    @Test
    public void testSmallDataAmount() throws IOException {
	Key rowKey1 = Key.of(1l);
	Key rowKey2 = Key.of(2l);
	Key rowKey3 = Key.of(3l);

	ColumnMap values1 = new ColumnMap();
	values1.put(Key.of(11l), ColumnValue.of(111l));
	values1.put(Key.of(12l), ColumnValue.of(112l));
	values1.put(Key.of(13l), ColumnValue.of(113l));

	ColumnMap values2 = new ColumnMap();
	values2.put(Key.of(21l), ColumnValue.of(211l));
	values2.put(Key.of(22l), ColumnValue.of(212l));

	ColumnMap values3 = new ColumnMap();
	values3.put(Key.of(31l), ColumnValue.of(311l));

	try (ColumnFamilyImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testSmallDataAmount", "testcf")) {

	    columnFamilyEngine.put(rowKey1, values1);
	    columnFamilyEngine.put(rowKey2, values2);
	    columnFamilyEngine.put(rowKey3, values3);
	    /*
	     * Add all 3 rows
	     */
	    ColumnMap returned1 = columnFamilyEngine.get(rowKey1);
	    assertEquals(3, returned1.size());
	    assertEquals(111l, Bytes.toLong(returned1.get(Key.of(11l)).getBytes()));
	    assertEquals(112l, Bytes.toLong(returned1.get(Key.of(12l)).getBytes()));
	    assertEquals(113l, Bytes.toLong(returned1.get(Key.of(13l)).getBytes()));
	    ColumnMap returned2 = columnFamilyEngine.get(rowKey2);
	    assertEquals(2, returned2.size());
	    assertEquals(211l, Bytes.toLong(returned2.get(Key.of(21l)).getBytes()));
	    assertEquals(212l, Bytes.toLong(returned2.get(Key.of(22l)).getBytes()));
	    ColumnMap returned3 = columnFamilyEngine.get(rowKey3);
	    assertEquals(1, returned3.size());
	    assertEquals(311l, Bytes.toLong(returned3.get(Key.of(31l)).getBytes()));

	    /*
	     * Update row1
	     */
	    values1 = new ColumnMap();
	    values1.put(Key.of(11l), ColumnValue.of(1111l));
	    values1.put(Key.of(12l), ColumnValue.of(1122l));
	    values1.put(Key.of(14l), ColumnValue.of(1144l));
	    columnFamilyEngine.put(rowKey1, values1);
	    /*
	     * Check for updates and not changed values
	     */
	    returned1 = columnFamilyEngine.get(rowKey1);
	    assertEquals(4, returned1.size());
	    assertEquals(1111l, Bytes.toLong(returned1.get(Key.of(11l)).getBytes()));
	    assertEquals(1122l, Bytes.toLong(returned1.get(Key.of(12l)).getBytes()));
	    assertEquals(113l, Bytes.toLong(returned1.get(Key.of(13l)).getBytes()));
	    assertEquals(1144l, Bytes.toLong(returned1.get(Key.of(14l)).getBytes()));
	    returned2 = columnFamilyEngine.get(rowKey2);
	    assertEquals(2, returned2.size());
	    assertEquals(211l, Bytes.toLong(returned2.get(Key.of(21l)).getBytes()));
	    assertEquals(212l, Bytes.toLong(returned2.get(Key.of(22l)).getBytes()));
	    returned3 = columnFamilyEngine.get(rowKey3);
	    assertEquals(1, returned3.size());
	    assertEquals(311l, Bytes.toLong(returned3.get(Key.of(31l)).getBytes()));
	    /*
	     * Delete row 2
	     */
	    columnFamilyEngine.delete(rowKey2);
	    /*
	     * Check for deleted row and unchanged values
	     */
	    returned1 = columnFamilyEngine.get(rowKey1);
	    assertEquals(4, returned1.size());
	    assertEquals(1111l, Bytes.toLong(returned1.get(Key.of(11l)).getBytes()));
	    assertEquals(1122l, Bytes.toLong(returned1.get(Key.of(12l)).getBytes()));
	    assertEquals(113l, Bytes.toLong(returned1.get(Key.of(13l)).getBytes()));
	    assertEquals(1144l, Bytes.toLong(returned1.get(Key.of(14l)).getBytes()));
	    returned2 = columnFamilyEngine.get(rowKey2);
	    assertTrue(returned2.isEmpty());
	    returned3 = columnFamilyEngine.get(rowKey3);
	    assertEquals(1, returned3.size());
	    assertEquals(311l, Bytes.toLong(returned3.get(Key.of(31l)).getBytes()));
	}
    }

    /**
     * This test checks the creation of a single data file.
     * 
     * @throws SchemaException
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void testSingleDataFileCreation() throws IOException {
	try (ColumnFamilyImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testSingleDataFileCreation",
		"testcf")) {
	    Storage storage = getStorage();
	    ColumnFamilyDescriptor columnFamilyDescriptor = getColumnFamilyDescriptor();
	    Set<File> commitLogs = getCommitLogs(storage, columnFamilyDescriptor.getDirectory());
	    assertEquals(1, commitLogs.size());
	    File commitLogFile = commitLogs.iterator().next();
	    columnFamilyEngine.setMaxCommitLogSize(1024 * 1024);
	    long rowKey = 0;
	    while ((commitLogs.size() == 1) && (storage.exists(commitLogFile))) {
		rowKey++;
		ColumnMap values = new ColumnMap();
		for (long i = 1; i <= 10; i++) {
		    values.put(Key.of(rowKey * i), ColumnValue.of(rowKey * i));
		}
		columnFamilyEngine.put(Key.of(rowKey), values);
		commitLogs.addAll(getCommitLogs(storage, columnFamilyDescriptor.getDirectory()));
	    }

	    ColumnMap columnMap = columnFamilyEngine.get(Key.of(2l));
	    assertNotNull("No column map loaded.", columnMap);
	    assertEquals(2l, Bytes.toLong(columnMap.get(Key.of(2l)).getBytes()));
	    assertEquals(4l, Bytes.toLong(columnMap.get(Key.of(4l)).getBytes()));
	    assertEquals(6l, Bytes.toLong(columnMap.get(Key.of(6l)).getBytes()));
	    assertEquals(8l, Bytes.toLong(columnMap.get(Key.of(8l)).getBytes()));
	    assertEquals(10l, Bytes.toLong(columnMap.get(Key.of(10l)).getBytes()));
	    assertEquals(12l, Bytes.toLong(columnMap.get(Key.of(12l)).getBytes()));
	    assertEquals(14l, Bytes.toLong(columnMap.get(Key.of(14l)).getBytes()));
	    assertEquals(16l, Bytes.toLong(columnMap.get(Key.of(16l)).getBytes()));
	    assertEquals(18l, Bytes.toLong(columnMap.get(Key.of(18l)).getBytes()));
	    assertEquals(20l, Bytes.toLong(columnMap.get(Key.of(20l)).getBytes()));
	}
	File dataFile = null;
	for (File file : getStorage().list(getColumnFamilyDescriptor().getDirectory(), new DataFilenameFilter())) {
	    if (dataFile == null) {
		dataFile = file;
	    } else {
		fail("Only one sstable file is expected.");
	    }
	}
	assertNotNull(dataFile);
	File indexFile = DataFileSet.getIndexName(dataFile);
	assertNotNull(indexFile);

	Storage storage = getStorage();
	try (IndexEntryIterable index = new IndexEntryIterable(storage.open(indexFile));
		DataFileReader reader = new DataFileReader(storage, dataFile)) {
	    Key currentRowKey = null;
	    long currentOffset = -1;
	    Iterator<IndexEntry> indexIterator = index.iterator();
	    Iterator<Row> dataIterator = reader.iterator();
	    while (indexIterator.hasNext() && dataIterator.hasNext()) {
		IndexEntry indexEntry = indexIterator.next();
		Row dataEntry = dataIterator.next();
		Key rowKey = indexEntry.getRowKey();
		assertEquals(rowKey, dataEntry.getKey());
		long offset = indexEntry.getOffset();
		assertTrue(currentOffset < offset);
		if (currentRowKey != null) {
		    if (currentRowKey.compareTo(rowKey) >= 0) {
			fail("Wrong key order for '" + currentRowKey + "' and '" + rowKey + "'.");
		    }
		}
		currentOffset = offset;
		currentRowKey = rowKey;
	    }
	}

    }

    @Test
    public void testMultiDataFileCreationWithCompaction() throws IOException, StorageException {
	try (ColumnFamilyImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testMultiDataFileCreationWithCompaction", "testcf")) {
	    Storage storage = getStorage();
	    ColumnFamilyDescriptor columnFamilyDescriptor = getColumnFamilyDescriptor();
	    Set<File> commitLogs = getCommitLogs(storage, columnFamilyDescriptor.getDirectory());
	    columnFamilyEngine.setMaxCommitLogSize(10 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(100 * 1024);
	    long rowKey = 0;
	    while (commitLogs.size() < 20) {
		rowKey++;
		ColumnMap values = new ColumnMap();
		for (long i = 1; i <= 10; i++) {
		    values.put(Key.of(rowKey * i), ColumnValue.of(rowKey * i));
		}
		columnFamilyEngine.put(Key.of(rowKey), values);
		commitLogs.addAll(getCommitLogs(storage, columnFamilyDescriptor.getDirectory()));
	    }
	}
	Set<File> dataFiles = new HashSet<>();
	Set<File> indexFiles = new HashSet<>();
	Storage storage = getStorage();
	ColumnFamilyDescriptor columnFamilyDescriptor = getColumnFamilyDescriptor();
	for (File file : storage.list(columnFamilyDescriptor.getDirectory())) {
	    if (file.getName().endsWith(LogStructuredStore.DATA_FILE_SUFFIX)) {
		dataFiles.add(file);
	    }
	    if (file.getName().endsWith(LogStructuredStore.INDEX_FILE_SUFFIX)) {
		indexFiles.add(file);
	    }
	}

	assertEquals(28, dataFiles.size());
	assertEquals(27, indexFiles.size());
    }

    @Test
    public void testLargerTriangularAmountOfData() throws IOException {
	try (ColumnFamilyImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testLargerTriangularAmountOfData",
		"testcf")) {
	    columnFamilyEngine.setMaxCommitLogSize(100 * 1024 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(1024 * 1024 * 1024);

	    final int TEST_SIZE = 500;
	    StopWatch writingTime = new StopWatch();
	    writingTime.start();
	    for (int i = 1; i <= TEST_SIZE; ++i) {
		ColumnMap columnMap = new ColumnMap();
		for (int j = i; j <= TEST_SIZE; ++j) {
		    columnMap.put(Key.of(i), ColumnValue.of(i + j));
		    columnFamilyEngine.put(Key.of(j), columnMap);
		}
	    }
	    writingTime.stop();
	    System.out.println("Writing time for test size '" + TEST_SIZE + "': " + writingTime.toString());
	    StopWatch readingTime = new StopWatch();
	    readingTime.start();
	    for (int i = 1; i <= TEST_SIZE; ++i) {
		ColumnMap columnMap = columnFamilyEngine.get(Key.of(i));
		assertNotNull(columnMap);
		for (int j = 1; j <= TEST_SIZE; ++j) {
		    ColumnValue value = columnMap.get(Key.of(j));
		    if (j > i) {
			assertNull(value);
		    } else {
			assertNotNull(value);
			assertEquals(i + j, Bytes.toInt(columnMap.get(Key.of(j)).getBytes()));
		    }
		}
	    }
	    readingTime.stop();
	    System.out.println("Reading time for test size '" + TEST_SIZE + "': " + readingTime.toString());

	}
    }
}
