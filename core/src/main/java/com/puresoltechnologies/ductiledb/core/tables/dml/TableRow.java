package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.storage.engine.Key;

/**
 * This class represents a single table row.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableRow {

    public Key getRowKey();

    public <T> T get(String columnName);

    public byte[] getBytes(String columnName);

    public String getString(String columnName);

}
