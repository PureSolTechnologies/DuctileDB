package com.puresoltechnologies.ductiledb.core.schema;

import org.apache.hadoop.hbase.util.Bytes;

public enum HBaseColumnFamily {

    METADATA("metadata"), //
    DEFINITION("definition"), //
    PROPERTIES("properties"), //
    VARIABLES("variables"), //
    TYPES("types"), //
    EDGES("edges"), //
    VERICES("vertices"), //
    INDEX("index"), //
    ;

    private final String name;
    private final byte[] nameBytes;

    HBaseColumnFamily(String name) {
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
