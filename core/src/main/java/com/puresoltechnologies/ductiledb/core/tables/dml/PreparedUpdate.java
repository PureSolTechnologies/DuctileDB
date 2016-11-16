package com.puresoltechnologies.ductiledb.core.tables.dml;

public interface PreparedUpdate extends PreparedWhereSelectionStatement {

    public void addValue(String columnFamily, String column, Object value);

}
