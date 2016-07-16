package com.puresoltechnologies.ductiledb.core.graph.schema;

public enum HBaseTable {

    METADATA("metadata"), //
    PROPERTY_DEFINITIONS("property_definitions"), //
    TYPE_DEFINITIONS("type_definitions"), //
    VERTICES("vertices"), //
    EDGES("edges"), //
    VERTEX_PROPERTIES("vertex_properties"), //
    VERTEX_TYPES("vertex_types"), //
    EDGE_PROPERTIES("edge_properties"), //
    EDGE_TYPES("edge_types");

    private final String name;

    HBaseTable(String name) {
	this.name = name;
    }

    public String getName() {
	return name;
    }
}
