package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyScanner;

public class IndexedColumnFamilyScannerImpl implements ColumnFamilyScanner {

    private final ColumnFamilyEngineImpl columnFamilyEngine;
    private final SecondaryIndexEngineImpl indexEngine;
    private final byte[] fromValue;
    private final byte[] toValue;
    private final ColumnFamilyScanner scanner;

    public IndexedColumnFamilyScannerImpl(ColumnFamilyEngineImpl columnFamilyEngine,
	    SecondaryIndexEngineImpl indexEngine, byte[] fromValue, byte[] toValue) {
	this.columnFamilyEngine = columnFamilyEngine;
	this.indexEngine = indexEngine;
	this.fromValue = fromValue;
	this.toValue = toValue;
	this.scanner = indexEngine.getScanner(fromValue, toValue);
    }

    @Override
    public ColumnFamilyRow peek() {
	Key key = scanner.peek().getRowKey();
	return new ColumnFamilyRow(key, columnFamilyEngine.get(key.getKey()));
    }

    @Override
    public boolean hasNext() {
	return scanner.hasNext();
    }

    @Override
    public ColumnFamilyRow next() {
	Key key = scanner.next().getRowKey();
	return new ColumnFamilyRow(key, columnFamilyEngine.get(key.getKey()));
    }

    @Override
    public void close() throws IOException {
	scanner.close();
    }

}
