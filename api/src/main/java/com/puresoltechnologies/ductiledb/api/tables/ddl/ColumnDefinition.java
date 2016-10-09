package com.puresoltechnologies.ductiledb.api.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;

public interface ColumnDefinition {

    public String getColumnFamily();

    public String getName();

    public ColumnType getType();

}
