package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;

public class ColumnFamilyScanner implements Closeable {

    private final ColumnFamilyEngineImpl columnFamilyEngine;
    private final NavigableSet<byte[]> columns = new TreeSet<>();

    public ColumnFamilyScanner(ColumnFamilyEngineImpl columnFamilyEngine, NavigableSet<byte[]> columns) {
	this.columnFamilyEngine = columnFamilyEngine;
	this.columns.addAll(columns);
	PeekingIterator<IndexEntry> indexIterator = columnFamilyEngine.getMemtableIterator();
    }

    @Override
    public void close() throws IOException {
	// TODO Auto-generated method stub
    }

}
