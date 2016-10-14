package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.core.tables.DuctileDBStatement;

public interface Insert extends DuctileDBStatement {

    public void addValue(String columnFamily, String column, Object value);

}
