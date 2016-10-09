package com.puresoltechnologies.ductiledb.api.tables.dml;

import com.puresoltechnologies.ductiledb.api.tables.DuctileDBStatement;

public interface Insert extends DuctileDBStatement {

    public void addValue(String columnFamily, String column, Object value);

}
