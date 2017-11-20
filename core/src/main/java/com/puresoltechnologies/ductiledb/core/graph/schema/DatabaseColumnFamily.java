package com.puresoltechnologies.ductiledb.core.graph.schema;

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;

public enum DatabaseColumnFamily {

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
    private final Key key;

    DatabaseColumnFamily(String name) {
	this.name = name;
	this.nameBytes = Bytes.fromString(this.name);
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
