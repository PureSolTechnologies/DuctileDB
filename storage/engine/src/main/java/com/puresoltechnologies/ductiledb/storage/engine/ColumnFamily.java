package com.puresoltechnologies.ductiledb.storage.engine;

public class ColumnFamily {

    private final ColumnFamilyEngine columnFamilyEngine;

    public ColumnFamily(ColumnFamilyEngine columnFamilyEngine) {
	super();
	this.columnFamilyEngine = columnFamilyEngine;
    }

    public ColumnFamilyEngine getEngine() {
	return columnFamilyEngine;
    }
}
