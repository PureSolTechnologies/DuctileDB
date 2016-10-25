package com.puresoltechnologies.ductiledb.core.tables.dml;

public interface PreparedInsert extends PreparedStatement {

    public void addValue(String columnFamily, String column, Object value);

}
