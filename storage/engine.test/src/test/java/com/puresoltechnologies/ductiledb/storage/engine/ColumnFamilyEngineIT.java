package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DbFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.ColumnFamilyRowIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableIndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableIndexIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableSet;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class ColumnFamilyEngineIT extends AbstractDatabaseEngineTest {

    @Test
    public void testSmallDataAmount() throws StorageException, SchemaException {
	DatabaseEngineImpl engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespaceIfNotPresent("testSmallDataAmount");
	TableDescriptor tableDescriptor = schemaManager.createTableIfNotPresent(namespace, "test");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamilyIfNotPresent(tableDescriptor,
		Bytes.toBytes("testcf"));

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

	byte[] timestamp = Bytes.toBytes(Instant.now());
	Table table = engine.getTable(tableDescriptor);
	ColumnFamily columnFamily = table.getColumnFamily(columnFamilyDescriptor);
	try (ColumnFamilyEngineImpl columnFamilyEngine = (ColumnFamilyEngineImpl) columnFamily.getEngine()) {
	    columnFamilyEngine.put(timestamp, rowKey1, values1);
	    columnFamilyEngine.put(timestamp, rowKey2, values2);
	    columnFamilyEngine.put(timestamp, rowKey3, values3);

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
	}
    }

    @Test
    public void testSSTableCreation() throws SchemaException, FileNotFoundException, IOException, StorageException {
	DatabaseEngineImpl engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespaceIfNotPresent("testSSTableCreation");
	TableDescriptor tableDescriptor = schemaManager.createTableIfNotPresent(namespace, "test");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamilyIfNotPresent(tableDescriptor,
		Bytes.toBytes("testcf"));
	Storage storage = engine.getStorage();

	Table table = engine.getTable(tableDescriptor);
	ColumnFamily columnFamily = table.getColumnFamily(columnFamilyDescriptor);
	try (ColumnFamilyEngineImpl columnFamilyEngine = (ColumnFamilyEngineImpl) columnFamily.getEngine()) {
	    Set<File> commitLogs = getCommitLogs(storage, columnFamilyDescriptor.getDirectory());
	    assertEquals(1, commitLogs.size());
	    File commitLogFile = commitLogs.iterator().next();
	    byte[] timestamp = Bytes.toBytes(Instant.now());
	    columnFamilyEngine.setMaxCommitLogSize(1024 * 1024);
	    long rowKey = 0;
	    while ((commitLogs.size() == 1) && (storage.exists(commitLogFile))) {
		rowKey++;
		ColumnMap values = new ColumnMap();
		for (long i = 1; i <= 10; i++) {
		    byte[] value = Bytes.toBytes(rowKey * i);
		    values.put(value, value);
		}
		columnFamilyEngine.put(timestamp, Bytes.toBytes(rowKey), values);
		commitLogs.addAll(getCommitLogs(storage, columnFamilyDescriptor.getDirectory()));
	    }

	    ColumnMap columnMap = columnFamilyEngine.get(Bytes.toBytes(2l));
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
	for (File file : storage.list(columnFamilyDescriptor.getDirectory(), new DbFilenameFilter())) {
	    if (dataFile == null) {
		dataFile = file;
	    } else {
		fail("Only one sstable file is expected.");
	    }
	}
	assertNotNull(dataFile);
	File indexFile = SSTableSet.getIndexName(dataFile);
	assertNotNull(indexFile);

	ByteArrayComparator comparator = ByteArrayComparator.getInstance();
	SSTableReader reader = new SSTableReader(storage, dataFile, indexFile);
	try (SSTableIndexIterable index = reader.readIndex(); ColumnFamilyRowIterable data = reader.readData()) {
	    byte[] currentRowKey = null;
	    long currentOffset = -1;
	    Iterator<SSTableIndexEntry> indexIterator = index.iterator();
	    Iterator<ColumnFamilyRow> dataIterator = data.iterator();
	    while (indexIterator.hasNext() && dataIterator.hasNext()) {
		SSTableIndexEntry indexEntry = indexIterator.next();
		ColumnFamilyRow dataEntry = dataIterator.next();
		byte[] rowKey = indexEntry.getRowKey();
		assertEquals(Bytes.toHumanReadableString(rowKey), Bytes.toHumanReadableString(dataEntry.getRowKey()));
		long offset = indexEntry.getOffset();
		assertTrue(currentOffset < offset);
		if (currentRowKey != null) {
		    if (comparator.compare(currentRowKey, rowKey) >= 0) {
			fail("Wrong key order for '" + Bytes.toHumanReadableString(currentRowKey) + "' and '"
				+ Bytes.toHumanReadableString(rowKey) + "'.");
		    }
		}
		currentOffset = offset;
		currentRowKey = rowKey;
	    }
	}

    }

    @Test
    public void testLargeSSTableCreationWithCompaction()
	    throws SchemaException, FileNotFoundException, IOException, StorageException {
	DatabaseEngineImpl engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespaceIfNotPresent("testSSTableCreationWithCompaction");
	TableDescriptor tableDescriptor = schemaManager.createTableIfNotPresent(namespace, "test");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamilyIfNotPresent(tableDescriptor,
		Bytes.toBytes("testcf"));

	Storage storage = engine.getStorage();

	Table table = engine.getTable(tableDescriptor);
	ColumnFamily columnFamily = table.getColumnFamily(columnFamilyDescriptor);
	try (ColumnFamilyEngineImpl columnFamilyEngine = (ColumnFamilyEngineImpl) columnFamily.getEngine()) {
	    Set<File> commitLogs = getCommitLogs(storage, columnFamilyDescriptor.getDirectory());
	    byte[] timestamp = Bytes.toBytes(Instant.now());
	    columnFamilyEngine.setMaxCommitLogSize(1024 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(10 * 1024 * 1024);
	    long rowKey = 0;
	    while (commitLogs.size() < 20) {
		rowKey++;
		ColumnMap values = new ColumnMap();
		for (long i = 1; i <= 10; i++) {
		    byte[] value = Bytes.toBytes(rowKey * i);
		    values.put(value, value);
		}
		columnFamilyEngine.put(timestamp, Bytes.toBytes(rowKey), values);
		commitLogs.addAll(getCommitLogs(storage, columnFamilyDescriptor.getDirectory()));
	    }
	}
	Set<File> dataFiles = new HashSet<>();
	Set<File> indexFiles = new HashSet<>();
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

}
