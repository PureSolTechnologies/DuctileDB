package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;

public class TableDefinitionImpl implements TableDefinition {

    private final String namespace;
    private final String name;
    private final String description;
    private List<ColumnDefinition<?>> primaryKey = new ArrayList<>();
    private Set<ColumnDefinition<?>> columns = new HashSet<>();

    public TableDefinitionImpl(String namespace, String name, String description) {
	this.namespace = namespace;
	this.name = name;
	this.description = description;
    }

    @Override
    public String getNamespace() {
	return namespace;
    }

    @Override
    public String getName() {
	return name;
    }

    @Override
    public String getDescription() {
	return description;
    }

    public void setPrimaryKey(String[] columns) {
	for (String column : columns) {
	    ColumnDefinition<?> columnDefinition = getColumnDefinition(column);
	    if (columnDefinition != null) {
		primaryKey.add(columnDefinition);
	    } else {
		throw new IllegalStateException("No definition for column '" + column + "' found.");
	    }
	}
    }

    @Override
    public Set<ColumnDefinition<?>> getColumnDefinitions() {
	return columns;
    }

    @Override
    public ColumnDefinition<?> getColumnDefinition(String columnName) {
	for (ColumnDefinition<?> columnDefinition : columns) {
	    if (columnDefinition.getName().equals(columnName)) {
		return columnDefinition;
	    }
	}
	return null;
    }

    @Override
    public List<ColumnDefinition<?>> getPrimaryKey() {
	return primaryKey;
    }

    public void addColumn(String columnFamily, String columnName, ColumnType type) {
	addColumn(columnFamily, columnName, type, "");
    }

    public void addColumn(String columnFamily, String columnName, ColumnType type, String description) {
	columns.add(new ColumnDefinitionImpl<>(columnFamily, columnName, type.getType(), description));
    }

    @Override
    public boolean isPrimaryKey(ColumnDefinition<?> columnDefinition) {
	for (ColumnDefinition<?> primaryKeyPart : primaryKey) {
	    if (primaryKeyPart.getName().equals(columnDefinition.getName())) {
		return true;
	    }
	}
	return false;
    }

}
