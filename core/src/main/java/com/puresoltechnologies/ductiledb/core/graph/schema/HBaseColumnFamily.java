package com.puresoltechnologies.ductiledb.core.graph.schema;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public enum HBaseColumnFamily {

    METADATA("metadata"), //
    VERTEX_DEFINITION("vertex_property"), //
    EDGE_DEFINITION("edge_property"), //
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
