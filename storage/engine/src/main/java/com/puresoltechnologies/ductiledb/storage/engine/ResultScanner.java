package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class ResultScanner implements Closeable, Iterable<Result> {

    private final Table table;
    private final Scan scan;

    public ResultScanner(Table table, Scan scan) {
	this.table = table;
	this.scan = scan;
    }

    @Override
    public Iterator<Result> iterator() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void close() throws IOException {
	// TODO Auto-generated method stub

    }

    public Object next() {
	// TODO Auto-generated method stub
	return null;
    }

}
