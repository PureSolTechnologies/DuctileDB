package com.puresoltechnologies.ductiledb.storage.engine.cf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class ColumnFamilyScannerIT extends AbstractColumnFamiliyEngineTest {

    private static String NAMESPACE = ColumnFamilyScannerIT.class.getSimpleName();

    @Test
    public void testEmptyScanner() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testEmptyScanner",
		"testcf")) {
	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	    assertNull(scanner.next());
	    scanner.skip();
	}
    }

    @Test
    public void testSingleEntryScannerWithoutBounds() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithoutBounds", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Bytes.toBytes(12l), Bytes.toBytes(123l));
	    byte[] rowKey = Bytes.toBytes(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(new RowKey(rowKey), result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Bytes.toBytes(12l)).getValue()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testSingleEntryScannerWithLowerBoundOnly() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithLowerBoundOnly", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Bytes.toBytes(12l), Bytes.toBytes(123l));
	    byte[] rowKey = Bytes.toBytes(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(rowKey, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(new RowKey(rowKey), result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Bytes.toBytes(12l)).getValue()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testSingleEntryScannerWithUpperBoundOnly() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithUpperBoundOnly", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Bytes.toBytes(12l), Bytes.toBytes(123l));
	    byte[] rowKey = Bytes.toBytes(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, rowKey);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(new RowKey(rowKey), result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Bytes.toBytes(12l)).getValue()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testSingleEntryScannerWithCloseBounds() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithCloseBounds", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Bytes.toBytes(12l), Bytes.toBytes(123l));
	    byte[] rowKey = Bytes.toBytes(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(rowKey, rowKey);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(new RowKey(rowKey), result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Bytes.toBytes(12l)).getValue()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testSingleEntryScannerWithWideBounds() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithWideBounds", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Bytes.toBytes(12l), Bytes.toBytes(123l));
	    byte[] rowKey = Bytes.toBytes(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(Bytes.toBytes(0l), Bytes.toBytes(123l));
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(new RowKey(rowKey), result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Bytes.toBytes(12l)).getValue()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testEmptyScannerAfterDeletion() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testEmptyScannerAfterDeletion", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Bytes.toBytes(123l), Bytes.toBytes(1234l));
	    byte[] rowKey = Bytes.toBytes(12345l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());

	    columnFamilyEngine.delete(rowKey);

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	    assertNull(scanner.next());
	    scanner.skip();
	}
    }

    @Test
    public void testEmptyScannerAfterDeletionWithCompaction() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testEmptyScannerAfterDeletionWithCompaction", "testcf")) {
	    columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(25 * 1024);
	    for (long i = 1; i <= 200; ++i) {
		ColumnMap values = new ColumnMap();
		values.put(Bytes.toBytes(i * 10), Bytes.toBytes(i * 100));
		byte[] rowKey = Bytes.toBytes(i);
		columnFamilyEngine.put(rowKey, values);
	    }

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());

	    for (long i = 1; i <= 200; ++i) {
		byte[] rowKey = Bytes.toBytes(i);
		columnFamilyEngine.delete(rowKey);
	    }

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	    assertNull(scanner.next());
	    scanner.skip();
	}
    }

    @Test
    public void testEmptyScannerAfterDeletionWithoutCompactionAfterRollover() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testEmptyScannerAfterDeletionWithoutCompactionAfterRollover", "testcf")) {
	    columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(25 * 1024);
	    columnFamilyEngine.setRunCompactions(false);
	    for (long i = 1; i <= 100; ++i) {
		ColumnMap values = new ColumnMap();
		values.put(Bytes.toBytes(i * 10), Bytes.toBytes(i * 100));
		byte[] rowKey = Bytes.toBytes(i);
		columnFamilyEngine.put(rowKey, values);
	    }

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());

	    for (long i = 1; i <= 100; ++i) {
		byte[] rowKey = Bytes.toBytes(i);
		columnFamilyEngine.delete(rowKey);
	    }

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	    assertNull(scanner.next());
	    scanner.skip();
	}
    }

    @Test
    public void testRowUpdateWithScanner() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testRowUpdateWithScanner",
		"testcf")) {
	    columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(25 * 1024);
	    // Check behavior of empty Memtable
	    byte[] rowKey = Bytes.toBytes(1l);
	    ColumnMap entry = columnFamilyEngine.get(rowKey);
	    assertTrue(entry.isEmpty());

	    // Check put
	    ColumnMap columns = new ColumnMap();
	    columns.put(Bytes.toBytes(10l), Bytes.toBytes(100l));
	    columnFamilyEngine.put(rowKey, columns);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(Bytes.toLong(rowKey), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertFalse(scanner.hasNext());

	    // Check put with column extension
	    columns = new ColumnMap();
	    columns.put(Bytes.toBytes(20l), Bytes.toBytes(200l));
	    columnFamilyEngine.put(rowKey, columns);

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(Bytes.toLong(rowKey), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertFalse(scanner.hasNext());

	    // Check put with column extensions with compaction
	    for (long i = 3; i <= 100; ++i) {
		columns = new ColumnMap();
		columns.put(Bytes.toBytes((i + 1) * 10), Bytes.toBytes((i + 1) * 100));
		columnFamilyEngine.put(rowKey, columns);
	    }

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(Bytes.toLong(rowKey), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertFalse(scanner.hasNext());
	}
    }

    @Test
    public void testRowDeleteWithScanner() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testRowDeleteWithScanner",
		"testcf")) {
	    columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(25 * 1024);
	    // Check behavior of empty Memtable
	    byte[] rowKey1 = Bytes.toBytes(1l);
	    ColumnMap entry1 = columnFamilyEngine.get(rowKey1);
	    assertTrue(entry1.isEmpty());

	    byte[] rowKey2 = Bytes.toBytes(2l);
	    ColumnMap entry2 = columnFamilyEngine.get(rowKey2);
	    assertTrue(entry2.isEmpty());

	    byte[] rowKey3 = Bytes.toBytes(3l);
	    ColumnMap entry3 = columnFamilyEngine.get(rowKey3);
	    assertTrue(entry3.isEmpty());

	    // Check put
	    ColumnMap columns = new ColumnMap();
	    columns.put(Bytes.toBytes(10l), Bytes.toBytes(100l));
	    columnFamilyEngine.put(rowKey1, columns);

	    ColumnMap columns2 = new ColumnMap();
	    columns2.put(Bytes.toBytes(20l), Bytes.toBytes(200l));
	    columnFamilyEngine.put(rowKey2, columns2);

	    ColumnMap columns3 = new ColumnMap();
	    columns3.put(Bytes.toBytes(30l), Bytes.toBytes(300l));
	    columnFamilyEngine.put(rowKey3, columns3);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(Bytes.toLong(rowKey1), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertEquals(Bytes.toLong(rowKey2), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertEquals(Bytes.toLong(rowKey3), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertFalse(scanner.hasNext());

	    // Check put with column extension
	    columnFamilyEngine.delete(rowKey2);

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(Bytes.toLong(rowKey1), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertEquals(Bytes.toLong(rowKey3), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertFalse(scanner.hasNext());

	    // Check after compaction
	    columnFamilyEngine.runCompaction();

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(Bytes.toLong(rowKey1), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertEquals(Bytes.toLong(rowKey3), Bytes.toLong(scanner.next().getRowKey().getKey()));
	    assertFalse(scanner.hasNext());
	}
    }

}
