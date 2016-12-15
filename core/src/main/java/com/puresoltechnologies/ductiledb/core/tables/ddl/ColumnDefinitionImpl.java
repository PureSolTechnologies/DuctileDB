package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypeDefinition;

public class ColumnDefinitionImpl<T> implements ColumnDefinition<T> {

    private final String columnFamily;
    private final String name;
    private final String description;
    private final ColumnTypeDefinition<T> type;

    public ColumnDefinitionImpl(String columnFamily, String name, ColumnTypeDefinition<T> type, String description) {
	super();
	this.columnFamily = columnFamily;
	this.name = name;
	this.type = type;
	this.description = description;
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
    public String getDescription() {
	return description;
    }

    @Override
    public ColumnTypeDefinition<T> getType() {
	return type;
    }

}
