package com.puresoltechnologies.ductiledb.core.tables.dml;

public interface PreparedInsert extends PreparedDMLStatement {

    public void addValue(String columnFamily, String column, Object value);

}
