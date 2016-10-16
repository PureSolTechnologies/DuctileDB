package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.core.tables.PreparedStatement;

public interface PreparedInsert extends PreparedStatement {

    public void addValue(String columnFamily, String column, Object value);

}
