package com.puresoltechnologies.ductiledb.bigtable;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.streaming.StreamIterator;

/**
 * This is class used to scan for results.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ResultScanner implements StreamIterator<Result>, CloseableIterable<Result> {

    private final NavigableMap<Key, ColumnFamilyScanner> cfScanners = new TreeMap<>();

    private final BigTable table;
    private Result nextResult = null;

    public ResultScanner(BigTableImpl table, Scan scan) {
	this.table = table;
	NavigableMap<Key, NavigableSet<Key>> columnFamilies = scan.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (Key columnFamilyKey : columnFamilies.keySet()) {
		ColumnFamily columnFamily = table.getColumnFamilyEngine(columnFamilyKey);
		cfScanners.put(columnFamilyKey, columnFamily.getScanner(scan.getStartRow(), scan.getEndRow()));
	    }
	} else {
	    for (ColumnFamily columnFamily : table.getColumnFamilyEngines()) {
		cfScanners.put(columnFamily.getName(), columnFamily.getScanner(scan.getStartRow(), scan.getEndRow()));
	    }
	}
    }

    public ResultScanner(BigTableImpl table, Scan scan, Key columnKey, ColumnValue value) {
	this.table = table;
	NavigableMap<Key, NavigableSet<Key>> columnFamilies = scan.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (Key columnFamilyKey : columnFamilies.keySet()) {
		ColumnFamily columnFamily = table.getColumnFamilyEngine(columnFamilyKey);
		cfScanners.put(columnFamilyKey, columnFamily.find(columnKey, value));
	    }
	} else {
	    for (ColumnFamily columnFamily : table.getColumnFamilyEngines()) {
		cfScanners.put(columnFamily.getName(), columnFamily.getScanner(scan.getStartRow(), scan.getEndRow()));
	    }
	}
    }

    public ResultScanner(BigTableImpl table, Scan scan, Key columnKey, ColumnValue fromValue, ColumnValue toValue) {
	this.table = table;
	NavigableMap<Key, NavigableSet<Key>> columnFamilies = scan.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (Key columnFamilyKey : columnFamilies.keySet()) {
		ColumnFamily columnFamily = table.getColumnFamilyEngine(columnFamilyKey);
		cfScanners.put(columnFamilyKey, columnFamily.find(columnKey, fromValue, toValue));
	    }
	} else {
	    for (ColumnFamily columnFamily : table.getColumnFamilyEngines()) {
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
	for (Entry<Key, ColumnFamilyScanner> scannerEntry : cfScanners.entrySet()) {
	    ColumnFamilyScanner scanner = scannerEntry.getValue();
	    ColumnFamilyRow row = scanner.peek();
	    if (row != null) {
		Key rowKey = row.getRowKey();
		if ((minimum == null) || (rowKey.compareTo(minimum) < 0)) {
		    minimum = rowKey;
		}
	    }
	}
	if (minimum == null) {
	    nextResult = null;
	    return;
	}
	Result result = new Result(minimum);
	for (Entry<Key, ColumnFamilyScanner> scannerEntry : cfScanners.entrySet()) {
	    ColumnFamilyScanner scanner = scannerEntry.getValue();
	    ColumnFamilyRow row = scanner.peek();
	    if (row != null) {
		Key rowKey = row.getRowKey();
		if (rowKey.compareTo(minimum) == 0) {
		    result.add(scannerEntry.getKey(), row.getColumnMap());
		    scanner.skip();
		}
	    }
	}
	nextResult = result;
    }

    @Override
    public StreamIterator<Result> iterator() {
	return this;
    }
}
