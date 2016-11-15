package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypeDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

/**
 * This class represents a single table row.
 * 
 * @author Rick-Rainer Ludwig
 */
public class TableRowImpl implements TableRow {

    private final Key rowKey;
    private final Map<String, byte[]> values = new HashMap<>();
    private final TableDefinition tableDefinition;

    public TableRowImpl(TableDefinition tableDefinition, Key rowKey) {
	this.tableDefinition = tableDefinition;
	this.rowKey = rowKey;
    }

    @Override
    public Key getRowKey() {
	return rowKey;
    }

    @Override
    public <T> T get(String columnName) {
	ColumnDefinition<?> columnDefinition = tableDefinition.getColumnDefinition(columnName);
	ColumnTypeDefinition<?> type = columnDefinition.getType();
	@SuppressWarnings("unchecked")
	T t = (T) type.fromBytes(values.get(columnName));
	return t;
    }

    @Override
    public byte[] getBytes(String columnName) {
	return values.get(columnName);
    }

    @Override
    public String getString(String column) {
	return Bytes.toString(values.get(column));
    }

    public void add(String column, byte[] value) {
	values.put(column, value);
    }

}
