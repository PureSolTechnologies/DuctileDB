package com.puresoltechnologies.ductiledb.core.tables.schema;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public enum DatabaseColumnFamily {

    ROWKEY("rowkey"), //
    METADATA("metadata"), //
    DEFINITION("definition"), //
    ;

    private final String name;
    private final byte[] nameBytes;
    private final Key key;

    DatabaseColumnFamily(String name) {
	this.name = name;
	this.nameBytes = Bytes.toBytes(this.name);
	this.key = Key.of(nameBytes);
    }

    public String getName() {
	return name;
    }

    public byte[] getNameBytes() {
	return nameBytes;
    }

    public Key getKey() {
	return key;
    }
}
