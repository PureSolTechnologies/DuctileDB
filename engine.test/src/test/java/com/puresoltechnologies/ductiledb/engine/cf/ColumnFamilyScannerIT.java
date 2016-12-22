package com.puresoltechnologies.ductiledb.engine.cf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

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
	    values.put(Key.of(12l), ColumnValue.of(123l));
	    Key rowKey = Key.of(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(rowKey, result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Key.of(12l)).getBytes()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testSingleEntryScannerWithLowerBoundOnly() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithLowerBoundOnly", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Key.of(12l), ColumnValue.of(123l));
	    Key rowKey = Key.of(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(rowKey, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(rowKey, result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Key.of(12l)).getBytes()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testSingleEntryScannerWithUpperBoundOnly() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithUpperBoundOnly", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Key.of(12l), ColumnValue.of(123l));
	    Key rowKey = Key.of(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, rowKey);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(rowKey, result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Key.of(12l)).getBytes()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testSingleEntryScannerWithCloseBounds() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithCloseBounds", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Key.of(12l), ColumnValue.of(123l));
	    Key rowKey = Key.of(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(rowKey, rowKey);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(rowKey, result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Key.of(12l)).getBytes()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testSingleEntryScannerWithWideBounds() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testSingleEntryScannerWithWideBounds", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Key.of(12l), ColumnValue.of(123l));
	    Key rowKey = Key.of(1l);
	    columnFamilyEngine.put(rowKey, values);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(Key.of(0l), Key.of(123l));
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertNotNull(scanner.peek());
	    ColumnFamilyRow result = scanner.next();
	    assertEquals(rowKey, result.getRowKey());
	    ColumnMap columnMap = result.getColumnMap();
	    assertEquals(1, columnMap.size());
	    assertEquals(123l, Bytes.toLong(columnMap.get(Key.of(12l)).getBytes()));

	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	}
    }

    @Test
    public void testEmptyScannerAfterDeletion() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE,
		"testEmptyScannerAfterDeletion", "testcf")) {
	    ColumnMap values = new ColumnMap();
	    values.put(Key.of(123l), ColumnValue.of(1234l));
	    Key rowKey = Key.of(12345l);
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
		values.put(Key.of(i * 10), ColumnValue.of(i * 100));
		Key rowKey = Key.of(i);
		columnFamilyEngine.put(rowKey, values);
	    }

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());

	    for (long i = 1; i <= 200; ++i) {
		Key rowKey = Key.of(i);
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
		values.put(Key.of(i * 10), ColumnValue.of(i * 100));
		Key rowKey = Key.of(i);
		columnFamilyEngine.put(rowKey, values);
	    }

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());

	    for (long i = 1; i <= 100; ++i) {
		Key rowKey = Key.of(i);
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
	    Key rowKey = Key.of(1l);
	    ColumnMap entry = columnFamilyEngine.get(rowKey);
	    assertTrue(entry.isEmpty());

	    // Check put
	    ColumnMap columns = new ColumnMap();
	    columns.put(Key.of(10l), ColumnValue.of(100l));
	    columnFamilyEngine.put(rowKey, columns);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(rowKey, scanner.next().getRowKey());
	    assertFalse(scanner.hasNext());

	    // Check put with column extension
	    columns = new ColumnMap();
	    columns.put(Key.of(20l), ColumnValue.of(200l));
	    columnFamilyEngine.put(rowKey, columns);

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(rowKey, scanner.next().getRowKey());
	    assertFalse(scanner.hasNext());

	    // Check put with column extensions with compaction
	    for (long i = 3; i <= 100; ++i) {
		columns = new ColumnMap();
		columns.put(Key.of((i + 1) * 10), ColumnValue.of((i + 1) * 100));
		columnFamilyEngine.put(rowKey, columns);
	    }

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(rowKey, scanner.next().getRowKey());
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
	    Key rowKey1 = Key.of(1l);
	    ColumnMap entry1 = columnFamilyEngine.get(rowKey1);
	    assertTrue(entry1.isEmpty());

	    Key rowKey2 = Key.of(2l);
	    ColumnMap entry2 = columnFamilyEngine.get(rowKey2);
	    assertTrue(entry2.isEmpty());

	    Key rowKey3 = Key.of(3l);
	    ColumnMap entry3 = columnFamilyEngine.get(rowKey3);
	    assertTrue(entry3.isEmpty());

	    // Check put
	    ColumnMap columns = new ColumnMap();
	    columns.put(Key.of(10l), ColumnValue.of(100l));
	    columnFamilyEngine.put(rowKey1, columns);

	    ColumnMap columns2 = new ColumnMap();
	    columns2.put(Key.of(20l), ColumnValue.of(200l));
	    columnFamilyEngine.put(rowKey2, columns2);

	    ColumnMap columns3 = new ColumnMap();
	    columns3.put(Key.of(30l), ColumnValue.of(300l));
	    columnFamilyEngine.put(rowKey3, columns3);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(rowKey1, scanner.next().getRowKey());
	    assertEquals(rowKey2, scanner.next().getRowKey());
	    assertEquals(rowKey3, scanner.next().getRowKey());
	    assertFalse(scanner.hasNext());

	    // Check put with column extension
	    columnFamilyEngine.delete(rowKey2);

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(rowKey1, scanner.next().getRowKey());
	    assertEquals(rowKey3, scanner.next().getRowKey());
	    assertFalse(scanner.hasNext());

	    // Check after compaction
	    columnFamilyEngine.runCompaction();

	    scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertTrue(scanner.hasNext());
	    assertEquals(rowKey1, scanner.next().getRowKey());
	    assertEquals(rowKey3, scanner.next().getRowKey());
	    assertFalse(scanner.hasNext());
	}
    }

    @Test
    public void testRange() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamilyEngine = createTestColumnFamily(NAMESPACE, "testScanner", "testcf")) {
	    columnFamilyEngine.setMaxCommitLogSize(5 * 1024);
	    columnFamilyEngine.setMaxDataFileSize(25 * 1024);

	    ColumnFamilyScanner scanner = columnFamilyEngine.getScanner(null, null);
	    assertNotNull(scanner);
	    assertFalse(scanner.hasNext());
	    assertNull(scanner.peek());
	    assertNull(scanner.next());

	    for (long i = 1; i <= 1000; ++i) {
		ColumnMap columnMap = new ColumnMap();
		columnMap.put(Key.of(i * 10), ColumnValue.of(i * 100));
		columnFamilyEngine.put(Key.of(i), columnMap);
	    }

	    scanner = columnFamilyEngine.getScanner(Key.of(100l), Key.of(900l));
	    assertNotNull(scanner);

	    long current = 100l;
	    while (scanner.hasNext()) {
		ColumnFamilyRow startResult = scanner.next();
		long l = Bytes.toLong(startResult.getRowKey().getBytes());
		assertEquals(current, l);
		ColumnValue value = startResult.getColumnMap().get(Key.of(l * 10l));
		assertNotNull(value);
		assertEquals(l * 100l, Bytes.toLong(value.getBytes()));
		++current;
	    }
	    assertEquals(901, current);

	}
    }
}
