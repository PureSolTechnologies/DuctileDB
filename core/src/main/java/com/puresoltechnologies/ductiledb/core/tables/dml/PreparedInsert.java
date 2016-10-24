package com.puresoltechnologies.ductiledb.core.tables.dml;

public interface PreparedInsert extends PreparedStatement {

    public void addValue(String columnFamily, String column, Object value);

    /**
     * Adds a value placeholder for a later bind.
     * 
     * @param columnFamily
     *            is the column family of the column to be used.
     * @param column
     *            is the name of the column.
     * @param index
     *            is the index counting from 1 which is to be used for later
     *            assignment.
     */
    public void addPlaceholder(String columnFamily, String column, int index);

}
