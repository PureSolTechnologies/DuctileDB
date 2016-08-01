package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class ResultScanner implements Closeable, Iterable<Result> {

    private final Table table;
    private final Scan scan;

    private final NavigableSet<ColumnFamily> cfEngines = new TreeSet<>();

    public ResultScanner(Table table, Scan scan) {
	this.table = table;
	this.scan = scan;
	TableDescriptor tableDescriptor = table.getTableDescriptor();
	for (byte[] columnFamily : scan.getColumnFamilies().keySet()) {
	    cfEngines.add(table.getColumnFamily(columnFamily));
	}
    }

    @Override
    public Iterator<Result> iterator() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void close() throws IOException {

    }

    public Object next() {
	// TODO Auto-generated method stub
	return null;
    }

}
