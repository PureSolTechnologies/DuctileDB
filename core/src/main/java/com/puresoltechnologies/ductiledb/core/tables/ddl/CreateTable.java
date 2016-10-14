package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.DuctileDBStatement;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;

public interface CreateTable extends DuctileDBStatement {

    public void addColumn(String columnFamily, String name, ColumnType type);

    public void setPrimaryKey(String... columns);

}
