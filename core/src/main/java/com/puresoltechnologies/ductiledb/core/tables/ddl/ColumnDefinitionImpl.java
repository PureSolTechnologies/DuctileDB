package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypeDefinition;

public class ColumnDefinitionImpl<T> implements ColumnDefinition<T> {

    private final String columnFamily;
    private final String name;
    private final ColumnTypeDefinition<T> type;

    public ColumnDefinitionImpl(String columnFamily, String name, ColumnTypeDefinition<T> type) {
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
    public ColumnTypeDefinition<T> getType() {
	return type;
    }

}
