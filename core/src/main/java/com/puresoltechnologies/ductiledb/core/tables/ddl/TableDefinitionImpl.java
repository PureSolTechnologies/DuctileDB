package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.ArrayList;
import java.util.List;

import com.puresoltechnologies.ductiledb.api.tables.ValueTypes;
import com.puresoltechnologies.ductiledb.api.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.api.tables.ddl.TableDefinition;

public class TableDefinitionImpl implements TableDefinition {

    private final String namespace;
    private final String name;
    private String[] primaryKey = new String[0];
    private List<ColumnDefinition> columns = new ArrayList<>();

    public TableDefinitionImpl(String namespace, String name) {
	this.namespace = namespace;
	this.name = name;
    }

    @Override
    public String getNamespace() {
	return namespace;
    }

    @Override
    public String getName() {
	return name;
    }

    public void addColumn(String columnFamily, String name, ValueTypes type) {
	columns.add(new ColumnDefinitionImpl(columnFamily, name, type));
    }

    public void setPrimaryKey(String[] columns) {
	primaryKey = columns;
    }

    public List<ColumnDefinition> getColumns() {
	return columns;
    }

}
