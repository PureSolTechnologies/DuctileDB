package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.NavigableMap;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class ResultScannerIT extends AbstractDatabaseEngineTest {

    private static final String NAMESPACE = ResultScannerIT.class.getSimpleName();

    @Test
    public void testEmptyScanner() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent(NAMESPACE);
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription,
		"testEmptyScanner");
	schemaManager.createColumnFamilyIfNotPresent(tableDescription, Bytes.toBytes("testcf"));
	TableEngine table = engine.getTable(tableDescription);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());
    }

    @Test
    public void testEmptyScannerAfterDeletion() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent(NAMESPACE);
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription,
		"testEmptyScannerAfterDeletion");
	ColumnFamilyDescriptor columnFamily = schemaManager.createColumnFamilyIfNotPresent(tableDescription,
		Bytes.toBytes("testcf"));
	TableEngine table = engine.getTable(tableDescription);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	Put put = new Put(Bytes.toBytes(1l));
	put.addColumn(columnFamily.getName(), Bytes.toBytes(12l), Bytes.toBytes(123l));
	table.put(put);

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertTrue(scanner.hasNext());
	assertNotNull(scanner.peek());

	table.delete(new Delete(Bytes.toBytes(1l)));

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());
    }

    @Test
    public void testEmptyScannerAfterDeletionWithCompactionMultiVertex1Property()
	    throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent(NAMESPACE);
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription,
		"testEmptyScannerAfterDeletionWithCompactionMultiVertex1Property");
	byte[] columnFamilyName = Bytes.toBytes("testcf");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamilyIfNotPresent(tableDescription,
		columnFamilyName);
	TableEngineImpl table = (TableEngineImpl) engine.getTable(tableDescription);
	ColumnFamilyEngineImpl columnFamilyEngine = table.getColumnFamilyEngine(columnFamilyName);
	columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	columnFamilyEngine.setMaxDataFileSize(25 * 1024);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	for (long i = 1; i <= 1000; ++i) {
	    Put put = new Put(Bytes.toBytes(i));
	    put.addColumn(columnFamilyDescriptor.getName(), Bytes.toBytes(i * 10), Bytes.toBytes(i * 100));
	    table.put(put);
	}

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertTrue(scanner.hasNext());
	assertNotNull(scanner.peek());

	for (long i = 1; i <= 1000; ++i) {
	    table.delete(new Delete(Bytes.toBytes(i)));
	}

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());
    }

    @Test
    public void testEmptyScannerAfterDeletionWithCompaction1VertexMultiColumns()
	    throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent(NAMESPACE);
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription,
		"testEmptyScannerAfterDeletionWithCompaction1VertexMultiColumns");
	byte[] columnFamilyName = Bytes.toBytes("testcf");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamilyIfNotPresent(tableDescription,
		columnFamilyName);
	TableEngineImpl table = (TableEngineImpl) engine.getTable(tableDescription);
	ColumnFamilyEngineImpl columnFamilyEngine = table.getColumnFamilyEngine(columnFamilyName);
	columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	columnFamilyEngine.setMaxDataFileSize(25 * 1024);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	for (long i = 1; i <= 1000; ++i) {
	    Put put = new Put(Bytes.toBytes(1l));
	    put.addColumn(columnFamilyDescriptor.getName(), Bytes.toBytes(i * 10), Bytes.toBytes(i * 100));
	    table.put(put);
	}

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertTrue(scanner.hasNext());
	assertNotNull(scanner.peek());

	table.delete(new Delete(Bytes.toBytes(1l)));

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());
    }

    @Test
    public void testScannerRangeScan() throws StorageException, SchemaException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent(NAMESPACE);
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription,
		"testScannerRangeScan");
	byte[] columnFamilyName = Bytes.toBytes("testcf");
	ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.createColumnFamilyIfNotPresent(tableDescription,
		columnFamilyName);
	TableEngineImpl table = (TableEngineImpl) engine.getTable(tableDescription);
	ColumnFamilyEngineImpl columnFamilyEngine = table.getColumnFamilyEngine(columnFamilyName);
	columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	columnFamilyEngine.setMaxDataFileSize(25 * 1024);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	for (long i = 1; i <= 1000; ++i) {
	    Put put = new Put(Bytes.toBytes(i));
	    put.addColumn(columnFamilyDescriptor.getName(), Bytes.toBytes(i * 10), Bytes.toBytes(i * 100));
	    table.put(put);
	}

	scanner = table.getScanner(new Scan(Bytes.toBytes(100l), Bytes.toBytes(900l)));
	assertNotNull(scanner);

	long current = 100l;
	while (scanner.hasNext()) {
	    Result startResult = scanner.next();
	    assertEquals(current, Bytes.toLong(startResult.getRowKey()));
	    NavigableMap<byte[], byte[]> familyMap = startResult.getFamilyMap(columnFamilyDescriptor.getName());
	    assertNotNull(familyMap);
	    assertEquals(current * 100l, Bytes.toLong(familyMap.get(Bytes.toBytes(current * 10))));
	    ++current;
	}
	assertEquals(901, current);
    }
}
