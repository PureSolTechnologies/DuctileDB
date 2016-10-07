package com.puresoltechnologies.ductiledb.core.tables.schema;

/**
 * This enum contains all tables created for RDBMS part of DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public enum DatabaseTable {

    METADATA("metadata"), //
    NAMESPACES("namespaces"), //
    TABLES("tables"), //
    ;

    private final String name;

    DatabaseTable(String name) {
	this.name = name;
    }

    public String getName() {
	return name;
    }
}
