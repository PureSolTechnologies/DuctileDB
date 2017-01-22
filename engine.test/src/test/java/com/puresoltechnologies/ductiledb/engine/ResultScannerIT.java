package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableMap;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.bigtable.BigTableImpl;
import com.puresoltechnologies.ductiledb.bigtable.Delete;
import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.bigtable.Result;
import com.puresoltechnologies.ductiledb.bigtable.ResultScanner;
import com.puresoltechnologies.ductiledb.bigtable.Scan;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyImpl;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class ResultScannerIT extends AbstractDatabaseEngineTest {

    private static final String NAMESPACE = ResultScannerIT.class.getSimpleName();

    @Test
    public void testEmptyScanner() throws IOException {
	DatabaseEngine engine = getEngine();

	Namespace namespace = engine.addNamespace(NAMESPACE);
	BigTable table = namespace.addTable("testEmptyScanner", "");
	table.addColumnFamily(Key.of("testcf"));

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());
    }

    @Test
    public void testEmptyScannerAfterDeletion() throws IOException {
	DatabaseEngine engine = getEngine();

	Namespace namespace = engine.addNamespace(NAMESPACE);
	BigTable table = namespace.addTable("testEmptyScannerAfterDeletion", "");
	ColumnFamily columnFamily = table.addColumnFamily(Key.of("testcf"));

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	Put put = new Put(Key.of(1l));
	put.addColumn(columnFamily.getName(), Key.of(12l), ColumnValue.of(123l));
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
    public void testEmptyScannerAfterDeletionWithCompactionMultiVertex1Property() throws IOException {
	DatabaseEngine engine = getEngine();
	Namespace namespace = engine.addNamespace(NAMESPACE);
	BigTableImpl table = (BigTableImpl) namespace
		.addTable("testEmptyScannerAfterDeletionWithCompactionMultiVertex1Property", "");
	Key columnFamilyName = Key.of("testcf");
	ColumnFamilyImpl columnFamily = (ColumnFamilyImpl) table.addColumnFamily(columnFamilyName);

	columnFamily.setMaxCommitLogSize(5 * 1024);
	columnFamily.setMaxDataFileSize(25 * 1024);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	for (long i = 1; i <= 1000; ++i) {
	    Put put = new Put(Key.of(i));
	    put.addColumn(columnFamily.getName(), Key.of(i * 10), ColumnValue.of(i * 100));
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
    public void testEmptyScannerAfterDeletionWithCompaction1VertexMultiColumns() throws IOException {
	DatabaseEngine engine = getEngine();
	Namespace namespace = engine.addNamespace(NAMESPACE);
	BigTableImpl table = (BigTableImpl) namespace
		.addTable("testEmptyScannerAfterDeletionWithCompaction1VertexMultiColumns", "");
	Key columnFamilyName = Key.of("testcf");
	ColumnFamilyImpl columnFamily = (ColumnFamilyImpl) table.addColumnFamily(columnFamilyName);

	columnFamily.setMaxCommitLogSize(5 * 1024);
	columnFamily.setMaxDataFileSize(25 * 1024);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	for (long i = 1; i <= 1000; ++i) {
	    Put put = new Put(Key.of(1l));
	    put.addColumn(columnFamily.getName(), Key.of(i * 10), ColumnValue.of(i * 100));
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
    public void testScannerRangeScan() throws IOException {
	DatabaseEngine engine = getEngine();
	Namespace namespace = engine.addNamespace(NAMESPACE);
	BigTableImpl table = (BigTableImpl) namespace.addTable("testScannerRangeScan", "");
	Key columnFamilyName = Key.of("testcf");
	ColumnFamilyImpl columnFamily = (ColumnFamilyImpl) table.addColumnFamily(columnFamilyName);

	columnFamily.setMaxCommitLogSize(5 * 1024);
	columnFamily.setMaxDataFileSize(25 * 1024);

	ResultScanner scanner = table.getScanner(new Scan());
	assertNotNull(scanner);
	assertFalse(scanner.hasNext());
	assertNull(scanner.peek());
	assertNull(scanner.next());

	for (long i = 1; i <= 1000; ++i) {
	    Put put = new Put(Key.of(i));
	    put.addColumn(columnFamily.getName(), Key.of(i * 10), ColumnValue.of(i * 100));
	    table.put(put);
	}

	scanner = table.getScanner(new Scan(Key.of(100l), Key.of(900l)));
	assertNotNull(scanner);

	long current = 100l;
	while (scanner.hasNext()) {
	    Result startResult = scanner.next();
	    assertEquals(current, startResult.getRowKey().toLong());
	    NavigableMap<Key, ColumnValue> familyMap = startResult.getFamilyMap(columnFamily.getName());
	    assertNotNull(familyMap);
	    assertEquals(current * 100l, familyMap.get(Key.of(current * 10)).toLong());
	    ++current;
	}
	assertEquals(901, current);
    }
}
