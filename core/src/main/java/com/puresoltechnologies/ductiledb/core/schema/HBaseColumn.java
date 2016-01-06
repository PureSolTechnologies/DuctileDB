package com.puresoltechnologies.ductiledb.core.schema;

import org.apache.hadoop.hbase.util.Bytes;

public enum HBaseColumn {

    VERTEX_ID("VertexId"), //
    START_VERTEX_ID("StartVertexId"), //
    TARGET_VERTEX_ID("TargetVertexId"), //
    EDGE_ID("EdgeId"), //
    SCHEMA_VERSION("SchemaVersion"), //
    ;

    private final String name;
    private final byte[] nameBytes;

    HBaseColumn(String name) {
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
