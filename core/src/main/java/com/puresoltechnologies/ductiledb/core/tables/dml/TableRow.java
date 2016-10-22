package com.puresoltechnologies.ductiledb.core.tables.dml;

/**
 * This class represents a single table row.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableRow {

    public <T> T get(String columnName);

    public byte[] getBytes(String columnName);

    public String getString(String columnName);

}
