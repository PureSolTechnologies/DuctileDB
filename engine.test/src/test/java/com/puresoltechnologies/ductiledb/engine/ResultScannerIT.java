package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.NavigableMap;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.engine.AbstractDatabaseEngineTest;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.Delete;
import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.Put;
import com.puresoltechnologies.ductiledb.engine.Result;
import com.puresoltechnologies.ductiledb.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.engine.Scan;
import com.puresoltechnologies.ductiledb.engine.TableEngine;
import com.puresoltechnologies.ductiledb.engine.TableEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class ResultScannerIT extends AbstractDatabaseEngineTest {

    private static final String NAMESPACE = ResultScannerIT.class.getSimpleName();

    @Test
    public void testEmptyScanner() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent(NAMESPACE);
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription,
		"testEmptyScanner", "");
	schemaManager.createColumnFamilyIfNotPresent(tableDescription, Key.of("testcf"));
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
		"testEmptyScannerAfterDeletion", "");
	ColumnFamilyDescriptor columnFamily = schemaManager.createColumnFamilyIfNotPresent(tableDescription,
		Key.of("testcf"));
	TableEngine table = engine.getTable(tableDescription);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	Put put = new Put(new Key(Bytes.toBytes(1l)));
	put.addColumn(columnFamily.getName(), new Key(Bytes.toBytes(12l)), ColumnValue.of(123l));
	table.put(put);

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertTrue(scanner.hasNext());
	assertNotNull(scanner.peek());

	table.delete(new Delete(Key.of(1l)));

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
		"testEmptyScannerAfterDeletionWithCompactionMultiVertex1Property", "");
	Key columnFamilyName = Key.of("testcf");
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
	    Put put = new Put(Key.of(i));
	    put.addColumn(columnFamilyDescriptor.getName(), Key.of(i * 10), ColumnValue.of(i * 100));
	    table.put(put);
	}

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertTrue(scanner.hasNext());
	assertNotNull(scanner.peek());

	for (long i = 1; i <= 1000; ++i) {
	    table.delete(new Delete(Key.of(i)));
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
		"testEmptyScannerAfterDeletionWithCompaction1VertexMultiColumns", "");
	Key columnFamilyName = Key.of("testcf");
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
	    Put put = new Put(Key.of(1l));
	    put.addColumn(columnFamilyDescriptor.getName(), Key.of(i * 10), ColumnValue.of(i * 100));
	    table.put(put);
	}

	scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertTrue(scanner.hasNext());
	assertNotNull(scanner.peek());

	table.delete(new Delete(Key.of(1l)));

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
		"testScannerRangeScan", "");
	Key columnFamilyName = Key.of("testcf");
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
	    Put put = new Put(Key.of(i));
	    put.addColumn(columnFamilyDescriptor.getName(), Key.of(i * 10), ColumnValue.of(i * 100));
	    table.put(put);
	}

	scanner = table.getScanner(new Scan(Key.of(100l), Key.of(900l)));
	assertNotNull(scanner);

	long current = 100l;
	while (scanner.hasNext()) {
	    Result startResult = scanner.next();
	    assertEquals(current, startResult.getRowKey().toLong());
	    NavigableMap<Key, ColumnValue> familyMap = startResult.getFamilyMap(columnFamilyDescriptor.getName());
	    assertNotNull(familyMap);
	    assertEquals(current * 100l, familyMap.get(Key.of(current * 10)).toLong());
	    ++current;
	}
	assertEquals(901, current);
    }
}
