package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.bigtable.TableEngineImpl;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypeDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.engine.CompoundKey;
import com.puresoltechnologies.ductiledb.engine.NamespaceImpl;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class PreparedInsertImpl extends AbstractPreparedDMLStatement implements PreparedInsert {

    private final String namespace;
    private final String table;
    private final Map<String, Map<String, InsertValue>> values = new HashMap<>();

    public PreparedInsertImpl(TableStoreImpl tableStore, TableDefinition tableDefinition) {
	super(tableStore, tableDefinition);
	this.namespace = tableDefinition.getNamespace();
	this.table = tableDefinition.getName();
    }

    @Override
    public void addValue(String columnFamily, String column, Object value) {
	Map<String, InsertValue> cf = values.get(columnFamily);
	if (cf == null) {
	    cf = new HashMap<>();
	    values.put(columnFamily, cf);
	}
	cf.put(column, new InsertValue(columnFamily, column, value));
    }

    @Override
    public TableRowIterable execute(Map<Integer, Comparable<?>> placeholderValues) {
	TableStoreImpl tableStore = getTableStore();
	NamespaceImpl namespaceEngine = tableStore.getStorageEngine().getNamespaceEngine(namespace);
	TableEngineImpl tableEngine = namespaceEngine.getTable(table);

	TableDefinition tableDefinition = getTableDefinition();
	List<ColumnDefinition<?>> primaryKey = tableDefinition.getPrimaryKey();
	byte[][] keyParts = new byte[primaryKey.size()][];
	for (int i = 0; i < primaryKey.size(); ++i) {
	    ColumnDefinition<?> primaryKeyPart = primaryKey.get(i);
	    InsertValue insertValue = null;
	    Map<String, InsertValue> insertValues = values.get(primaryKeyPart.getColumnFamily());
	    if (insertValues != null) {
		insertValue = insertValues.get(primaryKeyPart.getName());
	    }
	    Placeholder placeholder = null;
	    int placeholderIndex = getPlaceholderIndex(primaryKeyPart.getName());
	    if (placeholderIndex > 0) {
		placeholder = getPlaceholder(placeholderIndex);
	    }
	    if ((placeholder != null) && (insertValue != null)) {
		throw new IllegalStateException("Placeholder and value found.");
	    }
	    ColumnTypeDefinition<?> type = primaryKeyPart.getType();
	    if (placeholder != null) {
		keyParts[i] = type.toBytes(placeholderValues.get(placeholder.getIndex()));
	    } else {
		keyParts[i] = type.toBytes(insertValue.getValue());
	    }
	}
	Put put = new Put(CompoundKey.create(keyParts));
	// Add static values...
	for (Entry<String, Map<String, InsertValue>> columnFamilyEntry : values.entrySet()) {
	    for (Entry<String, InsertValue> columnEntry : columnFamilyEntry.getValue().entrySet()) {
		String columnFamily = columnFamilyEntry.getKey();
		String column = columnEntry.getKey();
		InsertValue value = columnEntry.getValue();
		addColumnValue(put, tableDefinition, columnFamily, column, value);
	    }
	}
	// Add dynamic values...
	for (Placeholder placeholder : getPlaceholders().values()) {
	    String columnFamily = placeholder.getColumnFamily();
	    String column = placeholder.getColumn();
	    Object value = placeholderValues.get(placeholder.getIndex());
	    addColumnValue(put, tableDefinition, columnFamily, column, value);
	}
	tableEngine.put(put);
	return null;
    }

    private void addColumnValue(Put put, TableDefinition tableDefinition, String columnFamily, String column,
	    Object value) {
	ColumnDefinition<?> columnDefinition = tableDefinition.getColumnDefinition(column);
	if (columnDefinition != null) {
	    if (tableDefinition.isPrimaryKey(columnDefinition)) {
		return;
	    }
	    ColumnTypeDefinition<?> type = columnDefinition.getType();
	    byte[] valueBytes = type.toBytes(value);
	    put.addColumn(Key.of(columnFamily), Key.of(column), ColumnValue.of(valueBytes));
	} else {
	    put.addColumn(Key.of(columnFamily), Key.of(column), ColumnValue.of((byte[]) value));
	}
    }

}
