package com.puresoltechnologies.ductiledb.core.tables.schema;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

/**
 * This enum contains all tables created for RDBMS part of DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public enum DatabaseTable {

    METADATA("metadata"), //
    NAMESPACES("namespaces"), //
    TABLES("tables"), //
    COLUMNS("columns"), //
    INDEXES("indexes"), //
    ;

    private final String name;
    private final byte[] nameBytes;

    DatabaseTable(String name) {
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
