package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

public class ColumnFamilyScanner implements Closeable {

    private final ColumnFamilyEngineImpl columnFamilyEngine;
    private final NavigableSet<byte[]> columns = new TreeSet<>();

    public ColumnFamilyScanner(ColumnFamilyEngineImpl columnFamilyEngine, NavigableSet<byte[]> columns) {
	this.columnFamilyEngine = columnFamilyEngine;
	this.columns.addAll(columns);
    }

    @Override
    public void close() throws IOException {
	// TODO Auto-generated method stub

    }

}
