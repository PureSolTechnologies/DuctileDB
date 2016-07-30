package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;

public class ColumnFamilyScanner implements Closeable {

    private final ColumnFamilyEngineImpl columnFamilyEngine;
    private final Scan scan;

    public ColumnFamilyScanner(ColumnFamilyEngineImpl columnFamilyEngine, Scan scan) {
	this.columnFamilyEngine = columnFamilyEngine;
	this.scan = scan;
    }

    @Override
    public void close() throws IOException {
	// TODO Auto-generated method stub

    }

}
