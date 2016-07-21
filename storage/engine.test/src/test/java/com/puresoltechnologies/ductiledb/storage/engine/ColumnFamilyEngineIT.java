package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableDataEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableDataIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableIndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableIndexIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableReader;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.DbFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class ColumnFamilyEngineIT extends AbstractDatabaseEngineTest {

    @Test
    public void testSmallDataAmount() throws StorageException, SchemaException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespace("testSmallDataAmount");
	TableDescriptor tableDescriptor = schemaManager.createTable(namespace, "test");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamily(tableDescriptor, "testcf");

	byte[] rowKey1 = Bytes.toBytes(1l);
	byte[] rowKey2 = Bytes.toBytes(2l);
	byte[] rowKey3 = Bytes.toBytes(3l);

	Map<byte[], byte[]> values1 = new HashMap<>();
	values1.put(Bytes.toBytes(11l), Bytes.toBytes(111l));
	values1.put(Bytes.toBytes(12l), Bytes.toBytes(112l));
	values1.put(Bytes.toBytes(13l), Bytes.toBytes(113l));

	Map<byte[], byte[]> values2 = new HashMap<>();
	values2.put(Bytes.toBytes(21l), Bytes.toBytes(211l));
	values2.put(Bytes.toBytes(22l), Bytes.toBytes(212l));

	Map<byte[], byte[]> values3 = new HashMap<>();
	values3.put(Bytes.toBytes(31l), Bytes.toBytes(311l));

	byte[] timestamp = Bytes.toBytes(Instant.now());
	try (ColumnFamilyEngine columnFamily = new ColumnFamilyEngine(engine.getStorage(), columnFamilyDescriptor,
		getConfiguration())) {
	    columnFamily.put(timestamp, rowKey1, values1);
	    columnFamily.put(timestamp, rowKey2, values2);
	    columnFamily.put(timestamp, rowKey3, values3);

	    Map<byte[], byte[]> returned1 = columnFamily.get(rowKey1);
	    assertEquals(3, returned1.size());
	    assertEquals(111l, Bytes.toLong(returned1.get(Bytes.toBytes(11l))));
	    assertEquals(112l, Bytes.toLong(returned1.get(Bytes.toBytes(12l))));
	    assertEquals(113l, Bytes.toLong(returned1.get(Bytes.toBytes(13l))));
	    Map<byte[], byte[]> returned2 = columnFamily.get(rowKey2);
	    assertEquals(2, returned2.size());
	    assertEquals(211l, Bytes.toLong(returned2.get(Bytes.toBytes(21l))));
	    assertEquals(212l, Bytes.toLong(returned2.get(Bytes.toBytes(22l))));
	    Map<byte[], byte[]> returned3 = columnFamily.get(rowKey3);
	    assertEquals(1, returned3.size());
	    assertEquals(311l, Bytes.toLong(returned3.get(Bytes.toBytes(31l))));
	}
    }

    @Test
    public void testSSTableCreation() throws SchemaException, FileNotFoundException, IOException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespace("testSSTableCreation");
	TableDescriptor tableDescriptor = schemaManager.createTable(namespace, "test");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamily(tableDescriptor, "testcf");
	Storage storage = engine.getStorage();

	DatabaseEngineConfiguration configuration = getConfiguration();
	try (ColumnFamilyEngine columnFamily = new ColumnFamilyEngine(engine.getStorage(), columnFamilyDescriptor,
		getConfiguration())) {
	    File commitLogFile = new File(columnFamilyDescriptor.getDirectory(), ColumnFamilyEngine.COMMIT_LOG_NAME);
	    // StopWatch stopWatch = new StopWatch();
	    // stopWatch.start();
	    byte[] timestamp = Bytes.toBytes(Instant.now());
	    columnFamily.setMaxCommitLogSize(1024 * 1024);
	    long commitLogSize = 0;
	    long lastCommitLogSize = 0;
	    long rowKey = 0;
	    while (lastCommitLogSize <= commitLogSize) {
		lastCommitLogSize = commitLogSize;
		rowKey++;
		Map<byte[], byte[]> values = new HashMap<>();
		for (long i = 1; i <= 10; i++) {
		    byte[] value = Bytes.toBytes(rowKey * i);
		    values.put(value, value);
		}
		columnFamily.put(timestamp, Bytes.toBytes(rowKey), values);
		FileStatus fileStatus = storage.getFileStatus(commitLogFile);
		commitLogSize = fileStatus.getLength();
		// stopWatch.stop();
		// System.out.println("count: " + rowKey + "; size: " +
		// commitLogSize + "; t=" + stopWatch.getMillis()
		// + "ms; perf=" + commitLogSize / 1024.0 /
		// stopWatch.getMillis() * 1000.0 + "kB/ms");
	    }
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
	File indexFile = SSTableReader.getIndexName(dataFile);
	assertNotNull(indexFile);

	ByteArrayComparator comparator = ByteArrayComparator.getInstance();
	SSTableReader reader = new SSTableReader(storage, dataFile, indexFile,
		configuration.getStorage().getBlockSize());
	try (SSTableIndexIterable index = reader.readIndex(); SSTableDataIterable data = reader.readData()) {
	    byte[] currentRowKey = null;
	    long currentOffset = -1;
	    Iterator<SSTableIndexEntry> indexIterator = index.iterator();
	    Iterator<SSTableDataEntry> dataIterator = data.iterator();
	    while (indexIterator.hasNext() && dataIterator.hasNext()) {
		SSTableIndexEntry indexEntry = indexIterator.next();
		SSTableDataEntry dataEntry = dataIterator.next();
		byte[] rowKey = indexEntry.getRowKey();
		assertEquals(Bytes.toHumanReadableString(rowKey), Bytes.toHumanReadableString(dataEntry.getRowKey()));
		long offset = indexEntry.getOffset();
		assertTrue(currentOffset < offset);
		if (currentRowKey != null) {
		    // System.out.println("Checking '" +
		    // Bytes.toHumanReadableString(currentRowKey) + "' and
		    // '"
		    // + Bytes.toHumanReadableString(rowKey) + "'.");
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
    public void testSSTableCreationWithCompaction()
	    throws SchemaException, FileNotFoundException, IOException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespace("testSSTableCreationWithCompaction");
	TableDescriptor tableDescriptor = schemaManager.createTable(namespace, "test");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamily(tableDescriptor, "testcf");
	Storage storage = engine.getStorage();

	try (ColumnFamilyEngine columnFamily = new ColumnFamilyEngine(engine.getStorage(), columnFamilyDescriptor,
		getConfiguration())) {
	    File commitLogFile = new File(columnFamilyDescriptor.getDirectory(), ColumnFamilyEngine.COMMIT_LOG_NAME);
	    // StopWatch stopWatch = new StopWatch();
	    // stopWatch.start();
	    byte[] timestamp = Bytes.toBytes(Instant.now());
	    columnFamily.setMaxCommitLogSize(1024 * 1024);
	    columnFamily.setMaxDataFileSize(1024 * 1024);
	    long commitLogSize = 0;
	    long lastCommitLogSize = 0;
	    long rowKey = 0;
	    int rolloverCount = 0;
	    while (rolloverCount < 3) {
		lastCommitLogSize = commitLogSize;
		rowKey++;
		Map<byte[], byte[]> values = new HashMap<>();
		for (long i = 1; i <= 10; i++) {
		    byte[] value = Bytes.toBytes(rowKey * i);
		    values.put(value, value);
		}
		columnFamily.put(timestamp, Bytes.toBytes(rowKey), values);
		FileStatus fileStatus = storage.getFileStatus(commitLogFile);
		commitLogSize = fileStatus.getLength();
		if (lastCommitLogSize > commitLogSize) {
		    ++rolloverCount;
		}
		// stopWatch.stop();
		// System.out.println("count: " + rowKey + "; size: " +
		// commitLogSize + "; t=" + stopWatch.getMillis()
		// + "ms; perf=" + commitLogSize / 1024.0 /
		// stopWatch.getMillis() * 1000.0 + "kB/ms");
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
	assertEquals(3, dataFiles.size());
	assertEquals(3, indexFiles.size());
    }

}
