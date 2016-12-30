package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.Statement;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;

public interface CreateTable extends Statement {

    public default void addColumn(String columnFamily, String name, ColumnType type) {
	addColumn(columnFamily, name, type, "");
    }

    public void addColumn(String columnFamily, String name, ColumnType type, String description);

    public void setPrimaryKey(String... columns);

}
