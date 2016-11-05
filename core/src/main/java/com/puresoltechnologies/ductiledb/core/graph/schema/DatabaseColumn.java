package com.puresoltechnologies.ductiledb.core.graph.schema;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

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
