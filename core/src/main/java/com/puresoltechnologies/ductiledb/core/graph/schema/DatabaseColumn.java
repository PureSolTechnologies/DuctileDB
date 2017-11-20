package com.puresoltechnologies.ductiledb.core.graph.schema;

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;

public enum DatabaseColumn {

    VERTEX_ID("VertexId"), //
    START_VERTEX_ID("StartVertexId"), //
    TARGET_VERTEX_ID("TargetVertexId"), //
    EDGE_ID("EdgeId"), //
    SCHEMA_VERSION("SchemaVersion"), //
    ;

    private final String name;
    private final byte[] nameBytes;
    private final Key key;

    DatabaseColumn(String name) {
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
