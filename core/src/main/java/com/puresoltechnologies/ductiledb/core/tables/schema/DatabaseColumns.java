package com.puresoltechnologies.ductiledb.core.tables.schema;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public enum DatabaseColumns {

    TYPE("type"), //
    COLUMN_FAMILY("column_family"), //
    ;

    private final String name;
    private final byte[] nameBytes;

    DatabaseColumns(String name) {
	this.name = name;
	this.nameBytes = Bytes.toBytes(this.name);
    }

    public String getName() {
	return name;
    }

    public byte[] getNameBytes() {
	return nameBytes;
    }
}