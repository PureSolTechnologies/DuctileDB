package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamily;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * This is class used to scan for results.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ResultScanner implements Closeable, PeekingIterator<Result>, Iterable<Result> {

    private static final ByteArrayComparator BYTE_ARRAY_COMPARATOR = ByteArrayComparator.getInstance();

    private final NavigableMap<byte[], ColumnFamilyScanner> cfScanners = new TreeMap<>(BYTE_ARRAY_COMPARATOR);

    private final Table table;
    private Result nextResult = null;

    public ResultScanner(Table table, Scan scan) {
	this.table = table;
	NavigableMap<byte[], NavigableSet<byte[]>> columnFamilies = scan.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (byte[] columnFamilyKey : columnFamilies.keySet()) {
		ColumnFamily columnFamily = table.getColumnFamily(columnFamilyKey);
		cfScanners.put(columnFamilyKey, columnFamily.getScanner(scan.getStartRow(), scan.getEndRow()));
	    }
	} else {
	    for (ColumnFamily columnFamily : table.getColumnFamilies()) {
		cfScanners.put(columnFamily.getName(), columnFamily.getScanner(scan.getStartRow(), scan.getEndRow()));
	    }
	}
    }

    public ResultScanner(Table table, Scan scan, byte[] columnKey, byte[] value) {
	this.table = table;
	NavigableMap<byte[], NavigableSet<byte[]>> columnFamilies = scan.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (byte[] columnFamilyKey : columnFamilies.keySet()) {
		ColumnFamily columnFamily = table.getColumnFamily(columnFamilyKey);
		cfScanners.put(columnFamilyKey, columnFamily.find(columnKey, value));
	    }
	} else {
	    for (ColumnFamily columnFamily : table.getColumnFamilies()) {
		cfScanners.put(columnFamily.getName(), columnFamily.getScanner(scan.getStartRow(), scan.getEndRow()));
	    }
	}
    }

    public ResultScanner(Table table, Scan scan, byte[] columnKey, byte[] fromValue, byte[] toValue) {
	this.table = table;
	NavigableMap<byte[], NavigableSet<byte[]>> columnFamilies = scan.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (byte[] columnFamilyKey : columnFamilies.keySet()) {
		ColumnFamily columnFamily = table.getColumnFamily(columnFamilyKey);
		cfScanners.put(columnFamilyKey, columnFamily.find(columnKey, fromValue, toValue));
	    }
	} else {
	    for (ColumnFamily columnFamily : table.getColumnFamilies()) {
		cfScanners.put(columnFamily.getName(), columnFamily.getScanner(scan.getStartRow(), scan.getEndRow()));
	    }
	}
    }

    @Override
    public void close() throws IOException {
	// intentionally left empty
    }

    @Override
    public boolean hasNext() {
	if (nextResult == null) {
	    readNextResult();
	}
	return nextResult != null;
    }

    @Override
    public Result next() {
	if (nextResult == null) {
	    readNextResult();
	}
	Result result = nextResult;
	nextResult = null;
	return result;
    }

    @Override
    public Result peek() {
	if (nextResult == null) {
	    readNextResult();
	}
	return nextResult;
    }

    private void readNextResult() {
	Key minimum = null;
	for (Entry<byte[], ColumnFamilyScanner> scannerEntry : cfScanners.entrySet()) {
	    ColumnFamilyScanner scanner = scannerEntry.getValue();
	    ColumnFamilyRow row = scanner.peek();
	    if (row != null) {
		Key rowKey = row.getRowKey();
		if ((minimum == null) || (BYTE_ARRAY_COMPARATOR.compare(rowKey.getKey(), minimum.getKey()) < 0)) {
		    minimum = rowKey;
		}
	    }
	}
	if (minimum == null) {
	    nextResult = null;
	    return;
	}
	Result result = new Result(minimum.getKey());
	for (Entry<byte[], ColumnFamilyScanner> scannerEntry : cfScanners.entrySet()) {
	    ColumnFamilyScanner scanner = scannerEntry.getValue();
	    ColumnFamilyRow row = scanner.peek();
	    if (row != null) {
		Key rowKey = row.getRowKey();
		if (BYTE_ARRAY_COMPARATOR.compare(rowKey.getKey(), minimum.getKey()) == 0) {
		    result.add(scannerEntry.getKey(), row.getColumnMap());
		    scanner.skip();
		}
	    }
	}
	nextResult = result;
    }

    @Override
    public PeekingIterator<Result> iterator() {
	return this;
    }
}
