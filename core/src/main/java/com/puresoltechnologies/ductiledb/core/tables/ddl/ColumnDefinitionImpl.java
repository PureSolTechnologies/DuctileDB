package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.api.tables.ddl.ColumnDefinition;

public class ColumnDefinitionImpl implements ColumnDefinition {

    private final String columnFamily;
    private final String name;
    private final ColumnType type;

    public ColumnDefinitionImpl(String columnFamily, String name, ColumnType type) {
	super();
	this.columnFamily = columnFamily;
	this.name = name;
	this.type = type;
    }

    @Override
    public String getColumnFamily() {
	return columnFamily;
    }

    @Override
    public String getName() {
	return name;
    }

    @Override
    public ColumnType getType() {
	return type;
    }

}
