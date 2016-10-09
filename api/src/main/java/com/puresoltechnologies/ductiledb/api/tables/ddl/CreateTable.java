package com.puresoltechnologies.ductiledb.api.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.DuctileDBStatement;
import com.puresoltechnologies.ductiledb.api.tables.ValueTypes;

public interface CreateTable extends DuctileDBStatement {

    public void addColumn(String columnFamily, String name, ValueTypes type);

    public void setPrimaryKey(String... columns);

}
