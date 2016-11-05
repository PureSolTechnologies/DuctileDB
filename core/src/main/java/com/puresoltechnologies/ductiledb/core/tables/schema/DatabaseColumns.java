package com.puresoltechnologies.ductiledb.core.tables.schema;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public enum DatabaseColumns {

    TYPE("type"), //
    COLUMN_FAMILY("column_family"), //
    CREATED("created"), //
    NAMESPACE("namespace"), //
    COLUMN("column"), //
    TABLE("table"), //
    PRIMARY_KEY_PART("primary_key_part"), //
    ;

    private final String name;
    private final byte[] nameBytes;
    private final Key key;

    DatabaseColumns(String name) {
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
