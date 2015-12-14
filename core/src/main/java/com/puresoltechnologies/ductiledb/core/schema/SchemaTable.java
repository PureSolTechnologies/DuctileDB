package com.puresoltechnologies.ductiledb.core.schema;

import org.apache.hadoop.hbase.TableName;

public enum SchemaTable {

    METADATA("metadata"), //
    VERTICES("vertices"), //
    EDGES("edges"), //
    VERTEX_PROPERTIES("vertex_properties"), //
    VERTEX_LABELS("vertex_labels"), //
    EDGE_PROPERTIES("edge_properties"), //
    EDGE_LABELS("edge_labels");

    private final String name;
    private final TableName tableName;

    SchemaTable(String name) {
	this.name = DuctileDBSchema.DUCTILEDB_NAMESPACE + ":" + name;
	this.tableName = TableName.valueOf(this.name);
    }

    public String getName() {
	return name;
    }

    public TableName getTableName() {
	return tableName;
    }
}
