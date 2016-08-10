package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * This is class used to scan for results.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ResultScanner implements Closeable, PeekingIterator<Result>, Iterable<Result> {

    private static final ByteArrayComparator BYTE_ARRAY_COMPARATOR = ByteArrayComparator.getInstance();

    private final NavigableSet<ColumnFamily> cfEngines = new TreeSet<>();
    private final NavigableMap<byte[], ColumnFamilyScanner> cfScanners = new TreeMap<>();

    private final Table table;
    private final Scan scan;
    private Result nextResult = null;

    public ResultScanner(Table table, Scan scan) throws StorageException {
	this.table = table;
	this.scan = scan;
	for (byte[] columnFamilyKey : scan.getColumnFamilies().keySet()) {
	    ColumnFamily columnFamily = table.getColumnFamily(columnFamilyKey);
	    cfEngines.add(columnFamily);
	    cfScanners.put(columnFamilyKey, columnFamily.getScanner(scan.getStartRow(), scan.getEndRow()));
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
	RowKey minimum = null;
	for (Entry<byte[], ColumnFamilyScanner> scannerEntry : cfScanners.entrySet()) {
	    ColumnFamilyScanner scanner = scannerEntry.getValue();
	    ColumnFamilyRow row = scanner.peek();
	    RowKey rowKey = row.getRowKey();
	    if ((minimum == null) || (BYTE_ARRAY_COMPARATOR.compare(rowKey.getKey(), minimum.getKey()) < 0)) {
		minimum = rowKey;
	    }
	}
	if (minimum == null) {
	    return;
	}
	Result result = new Result(minimum.getKey());
	for (Entry<byte[], ColumnFamilyScanner> scannerEntry : cfScanners.entrySet()) {
	    ColumnFamilyScanner scanner = scannerEntry.getValue();
	    ColumnFamilyRow row = scanner.peek();
	    RowKey rowKey = row.getRowKey();
	    if (BYTE_ARRAY_COMPARATOR.compare(rowKey.getKey(), minimum.getKey()) == 0) {
		result.add(scannerEntry.getKey(), row.getColumnMap());
		scanner.skip();
	    }
	}
	nextResult = result;
    }

    @Override
    public PeekingIterator<Result> iterator() {
	return this;
    }
}
