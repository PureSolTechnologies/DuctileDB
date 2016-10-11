package com.puresoltechnologies.ductiledb.core.tables.schema;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public enum DatabaseColumnFamily {

    ROWKEY(""), //
    METADATA("metadata"), //
    DEFINITION("definition"), //
    ;

    private final String name;
    private final byte[] nameBytes;

    DatabaseColumnFamily(String name) {
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
