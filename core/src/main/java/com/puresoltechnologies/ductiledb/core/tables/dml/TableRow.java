package com.puresoltechnologies.ductiledb.core.tables.dml;

/**
 * This class represents a single table row.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableRow {

    public byte[] getBytes(String column);

    public String getString(String column);

}
