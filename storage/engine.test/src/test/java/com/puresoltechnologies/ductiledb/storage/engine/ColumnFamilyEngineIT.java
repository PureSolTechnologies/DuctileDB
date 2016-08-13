package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DataFileSet;
import com.puresoltechnologies.ductiledb.storage.engine.io.DataFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.DataFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class ColumnFamilyEngineIT extends AbstractColumnFamiliyEngineTest {

    private static String NAMESPACE = ColumnFamilyEngineIT.class.getSimpleName();

    @Test
    public void testMemtableCRUD() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testMemtableCRUD",
		"testcf")) {
	    // Check behavior of empty Memtable
	    ColumnMap entry = columnFamilyEngine.get(Bytes.toBytes(0l));
	    assertTrue(entry.isEmpty());
	    columnFamilyEngine.delete(Bytes.toBytes(0l));
	    // Check put
	    ColumnMap columns = new ColumnMap();
	    columns.put(Bytes.toBytes(123l), Bytes.toBytes(123l));
	    columnFamilyEngine.put(Bytes.toBytes(0l), columns);
	    entry = columnFamilyEngine.get(Bytes.toBytes(0l));
	    assertNotNull(entry);
	    assertEquals(columns, entry);
	    // Check put of new value does not change former
	    ColumnMap columns2 = new ColumnMap();
	    columns2.put(Bytes.toBytes(1234l), Bytes.toBytes(1234l));
	    columnFamilyEngine.put(Bytes.toBytes(1l), columns2);
	    entry = columnFamilyEngine.get(Bytes.toBytes(0l));
	    assertNotNull(entry);
	    assertEquals(columns, entry);
	    entry = columnFamilyEngine.get(Bytes.toBytes(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);
	    // Check put
	    ColumnMap columns3 = new ColumnMap();
	    columns3.put(Bytes.toBytes(12345l), Bytes.toBytes(12345l));
	    columnFamilyEngine.put(Bytes.toBytes(0l), columns3);
	    entry = columnFamilyEngine.get(Bytes.toBytes(0l));
	    assertNotNull(entry);
	    columns3.put(Bytes.toBytes(123l), Bytes.toBytes(123l));
	    assertEquals(columns3, entry);
	    entry = columnFamilyEngine.get(Bytes.toBytes(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);
	    // Check put
	    ColumnMap columns4 = new ColumnMap();
	    columns4.put(Bytes.toBytes(12345l), Bytes.toBytes(123456l));
	    columnFamilyEngine.put(Bytes.toBytes(0l), columns4);
	    entry = columnFamilyEngine.get(Bytes.toBytes(0l));
	    assertNotNull(entry);
	    columns4.put(Bytes.toBytes(123l), Bytes.toBytes(123l));
	    assertEquals(columns4, entry);
	    entry = columnFamilyEngine.get(Bytes.toBytes(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);
	    // Check delete
	    HashSet<byte[]> columnsToDelete = new HashSet<>();
	    columnsToDelete.add(Bytes.toBytes(123l));
	    columnFamilyEngine.delete(Bytes.toBytes(0l), columnsToDelete);
	    entry = columnFamilyEngine.get(Bytes.toBytes(0l));
	    assertNotNull(entry);
	    ColumnMap columns5 = new ColumnMap();
	    columns5.put(Bytes.toBytes(12345l), Bytes.toBytes(123456l));
	    assertEquals(columns5, entry);
	    entry = columnFamilyEngine.get(Bytes.toBytes(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);
	    // Check delete
	    columnFamilyEngine.delete(Bytes.toBytes(0l));
	    entry = columnFamilyEngine.get(Bytes.toBytes(0l));
	    assertTrue(entry.isEmpty());
	    entry = columnFamilyEngine.get(Bytes.toBytes(1l));
	    assertNotNull(entry);
	    assertEquals(columns2, entry);

	}
    }

    @Test
    public void testWideRow() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testWideRow", "testcf")) {
	    ColumnMap entry = columnFamilyEngine.get(Bytes.toBytes(12345l));
	    assertTrue(entry.isEmpty());
	    columnFamilyEngine.delete(Bytes.toBytes(12345l));

	    ColumnMap columnMap = new ColumnMap();
	    for (int i = 1; i <= 100000; i++) {
		columnMap.put(Bytes.toBytes((long) i), Bytes.toBytes((long) i));
	    }
	    columnFamilyEngine.put(Bytes.toBytes(12345l), columnMap);

	    // Check put
	    entry = columnFamilyEngine.get(Bytes.toBytes(12345l));
	    assertNotNull(entry);
	    assertEquals(columnMap, entry);
	}
    }

    /**
     * This test checks for a small amount of data in memory functionality like
     * put, get, re-put and delete.
     * 
     * @throws StorageException
     * @throws SchemaException
     */
    @Test
    public void testSmallDataAmount() throws StorageException, SchemaException {
	byte[] rowKey1 = Bytes.toBytes(1l);
	byte[] rowKey2 = Bytes.toBytes(2l);
	byte[] rowKey3 = Bytes.toBytes(3l);

	ColumnMap values1 = new ColumnMap();
	values1.put(Bytes.toBytes(11l), Bytes.toBytes(111l));
	values1.put(Bytes.toBytes(12l), Bytes.toBytes(112l));
	values1.put(Bytes.toBytes(13l), Bytes.toBytes(113l));

	ColumnMap values2 = new ColumnMap();
	values2.put(Bytes.toBytes(21l), Bytes.toBytes(211l));
	values2.put(Bytes.toBytes(22l), Bytes.toBytes(212l));

	ColumnMap values3 = new ColumnMap();
	values3.put(Bytes.toBytes(31l), Bytes.toBytes(311l));

	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testSmallDataAmount",
		"testcf")) {

	    columnFamilyEngine.put(rowKey1, values1);
	    columnFamilyEngine.put(rowKey2, values2);
	    columnFamilyEngine.put(rowKey3, values3);
	    /*
	     * Add all 3 rows
	     */
	    ColumnMap returned1 = columnFamilyEngine.get(rowKey1);
	    assertEquals(3, returned1.size());
	    assertEquals(111l, Bytes.toLong(returned1.get(Bytes.toBytes(11l))));
	    assertEquals(112l, Bytes.toLong(returned1.get(Bytes.toBytes(12l))));
	    assertEquals(113l, Bytes.toLong(returned1.get(Bytes.toBytes(13l))));
	    ColumnMap returned2 = columnFamilyEngine.get(rowKey2);
	    assertEquals(2, returned2.size());
	    assertEquals(211l, Bytes.toLong(returned2.get(Bytes.toBytes(21l))));
	    assertEquals(212l, Bytes.toLong(returned2.get(Bytes.toBytes(22l))));
	    ColumnMap returned3 = columnFamilyEngine.get(rowKey3);
	    assertEquals(1, returned3.size());
	    assertEquals(311l, Bytes.toLong(returned3.get(Bytes.toBytes(31l))));

	    /*
	     * Update row1
	     */
	    values1 = new ColumnMap();
	    values1.put(Bytes.toBytes(11l), Bytes.toBytes(1111l));
	    values1.put(Bytes.toBytes(12l), Bytes.toBytes(1122l));
	    values1.put(Bytes.toBytes(14l), Bytes.toBytes(1144l));
	    columnFamilyEngine.put(rowKey1, values1);
	    /*
	     * Check for updates and not changed values
	     */
	    returned1 = columnFamilyEngine.get(rowKey1);
	    assertEquals(4, returned1.size());
	    assertEquals(1111l, Bytes.toLong(returned1.get(Bytes.toBytes(11l))));
	    assertEquals(1122l, Bytes.toLong(returned1.get(Bytes.toBytes(12l))));
	    assertEquals(113l, Bytes.toLong(returned1.get(Bytes.toBytes(13l))));
	    assertEquals(1144l, Bytes.toLong(returned1.get(Bytes.toBytes(14l))));
	    returned2 = columnFamilyEngine.get(rowKey2);
	    assertEquals(2, returned2.size());
	    assertEquals(211l, Bytes.toLong(returned2.get(Bytes.toBytes(21l))));
	    assertEquals(212l, Bytes.toLong(returned2.get(Bytes.toBytes(22l))));
	    returned3 = columnFamilyEngine.get(rowKey3);
	    assertEquals(1, returned3.size());
	    assertEquals(311l, Bytes.toLong(returned3.get(Bytes.toBytes(31l))));
	    /*
	     * Delete row 2
	     */
	    columnFamilyEngine.delete(rowKey2);
	    /*
	     * Check for deleted row and unchanged values
	     */
	    returned1 = columnFamilyEngine.get(rowKey1);
	    assertEquals(4, returned1.size());
	    assertEquals(1111l, Bytes.toLong(returned1.get(Bytes.toBytes(11l))));
	    assertEquals(1122l, Bytes.toLong(returned1.get(Bytes.toBytes(12l))));
	    assertEquals(113l, Bytes.toLong(returned1.get(Bytes.toBytes(13l))));
	    assertEquals(1144l, Bytes.toLong(returned1.get(Bytes.toBytes(14l))));
	    returned2 = columnFamilyEngine.get(rowKey2);
	    assertTrue(returned2.isEmpty());
	    returned3 = columnFamilyEngine.get(rowKey3);
	    assertEquals(1, returned3.size());
	    assertEquals(311l, Bytes.toLong(returned3.get(Bytes.toBytes(31l))));
	}
    }

    /**
     * This test checks the creation of a single data file.
     * 
     * @throws SchemaException
     * @throws FileNotFoundException
     * @throws IOException
     * @throws StorageException
     */
    @Test
    public void testSingleDataFileCreation()
	    throws SchemaException, FileNotFoundException, IOException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testSingleDataFileCreation",
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
		    byte[] value = Bytes.toBytes(rowKey * i);
		    values.put(value, value);
		}
		columnFamilyEngine.put(Bytes.toBytes(rowKey), values);
		commitLogs.addAll(getCommitLogs(storage, columnFamilyDescriptor.getDirectory()));
	    }

	    ColumnMap columnMap = columnFamilyEngine.get(Bytes.toBytes(2l));
	    assertNotNull("No column map loaded.", columnMap);
	    assertEquals(2l, Bytes.toLong(columnMap.get(Bytes.toBytes(2l))));
	    assertEquals(4l, Bytes.toLong(columnMap.get(Bytes.toBytes(4l))));
	    assertEquals(6l, Bytes.toLong(columnMap.get(Bytes.toBytes(6l))));
	    assertEquals(8l, Bytes.toLong(columnMap.get(Bytes.toBytes(8l))));
	    assertEquals(10l, Bytes.toLong(columnMap.get(Bytes.toBytes(10l))));
	    assertEquals(12l, Bytes.toLong(columnMap.get(Bytes.toBytes(12l))));
	    assertEquals(14l, Bytes.toLong(columnMap.get(Bytes.toBytes(14l))));
	    assertEquals(16l, Bytes.toLong(columnMap.get(Bytes.toBytes(16l))));
	    assertEquals(18l, Bytes.toLong(columnMap.get(Bytes.toBytes(18l))));
	    assertEquals(20l, Bytes.toLong(columnMap.get(Bytes.toBytes(20l))));
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
	    RowKey currentRowKey = null;
	    long currentOffset = -1;
	    Iterator<IndexEntry> indexIterator = index.iterator();
	    Iterator<ColumnFamilyRow> dataIterator = reader.iterator();
	    while (indexIterator.hasNext() && dataIterator.hasNext()) {
		IndexEntry indexEntry = indexIterator.next();
		ColumnFamilyRow dataEntry = dataIterator.next();
		RowKey rowKey = indexEntry.getRowKey();
		assertEquals(rowKey, dataEntry.getRowKey());
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
    public void testMultiDataFileCreationWithCompaction()
	    throws SchemaException, FileNotFoundException, IOException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testMultiDataFileCreationWithCompaction", "testcf")) {
	    Storage storage = getStorage();
	    ColumnFamilyDescriptor columnFamilyDescriptor = getColumnFamilyDescriptor();
	    Set<File> commitLogs = getCommitLogs(storage, columnFamilyDescriptor.getDirectory());
	    columnFamilyEngine.setMaxCommitLogSize(100 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(1024 * 1024);
	    long rowKey = 0;
	    while (commitLogs.size() < 20) {
		rowKey++;
		ColumnMap values = new ColumnMap();
		for (long i = 1; i <= 10; i++) {
		    byte[] value = Bytes.toBytes(rowKey * i);
		    values.put(value, value);
		}
		columnFamilyEngine.put(Bytes.toBytes(rowKey), values);
		commitLogs.addAll(getCommitLogs(storage, columnFamilyDescriptor.getDirectory()));
	    }
	}
	Set<File> dataFiles = new HashSet<>();
	Set<File> indexFiles = new HashSet<>();
	Storage storage = getStorage();
	ColumnFamilyDescriptor columnFamilyDescriptor = getColumnFamilyDescriptor();
	for (File file : storage.list(columnFamilyDescriptor.getDirectory())) {
	    if (file.getName().endsWith(ColumnFamilyEngine.DATA_FILE_SUFFIX)) {
		dataFiles.add(file);
	    }
	    if (file.getName().endsWith(ColumnFamilyEngine.INDEX_FILE_SUFFIX)) {
		indexFiles.add(file);
	    }
	}

	assertEquals(9, dataFiles.size());
	assertEquals(8, indexFiles.size());
    }

    @Test
    public void testResultScanner() throws StorageException, SchemaException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testResultScanner",
		"testcf")) {
	    columnFamilyEngine.setMaxCommitLogSize(100 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(1024 * 1024);
	    long key = 1;
	    int step = 0;
	    for (int i = 0; i < 5000; ++i) {
		key += step;
		++step;
		ColumnMap columns = new ColumnMap();
		columns.put(Bytes.toBytes(i), Bytes.toBytes(i));
		columnFamilyEngine.put(Bytes.toBytes(key), columns);
	    }
	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(Bytes.toBytes(0l), Bytes.toBytes(key));
	    while (scanner.hasNext()) {
		System.out.println(Bytes.toHumanReadableString(scanner.next().getRowKey().getKey()));
	    }
	}
    }

}
