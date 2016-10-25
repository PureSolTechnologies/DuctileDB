package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypeDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.CompoundKey;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class PreparedInsertImpl extends AbstractPreparedStatementImpl implements PreparedInsert {

    private final String namespace;
    private final String table;
    private final Map<String, Map<String, InsertValue>> values = new HashMap<>();

    public PreparedInsertImpl(TableDefinition tableDefinition) {
	super(tableDefinition);
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
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Object> placeholderValue) {
	NamespaceEngineImpl namespaceEngine = ((TableStoreImpl) tableStore).getStorageEngine()
		.getNamespaceEngine(namespace);
	TableEngineImpl tableEngine = namespaceEngine.getTableEngine(table);

	TableDefinition tableDefinition = getTableDefinition();
	List<ColumnDefinition<?>> primaryKey = tableDefinition.getPrimaryKey();
	byte[][] keyParts = new byte[primaryKey.size()][];
	for (int i = 0; i < primaryKey.size(); ++i) {
	    ColumnDefinition<?> primaryKeyPart = primaryKey.get(i);
	    InsertValue insertValue = null;
	    Placeholder placeholder = null;
	    Map<String, InsertValue> insertValues = values.get(primaryKeyPart.getColumnFamily());
	    if (insertValues != null) {
		insertValue = insertValues.get(primaryKeyPart.getName());
	    }
	    int placeholderIndex = getPlaceholderIndex(primaryKeyPart.getName());
	    if (placeholderIndex >= 0) {
		placeholder = getPlaceholder(placeholderIndex);
	    }
	    if ((placeholder != null) && (insertValue != null)) {
		throw new IllegalStateException("Placeholder and value found.");
	    }
	    Object value;
	    if (placeholder != null) {
		value = placeholderValue.get(placeholder.getIndex());
	    } else {
		value = insertValue.getValue();
	    }
	    keyParts[i] = primaryKeyPart.getType().toBytes(value);
	}
	Put put = new Put(CompoundKey.create(keyParts).getKey());
	for (Entry<String, Map<String, InsertValue>> columnFamilyEntry : values.entrySet()) {
	    for (Entry<String, InsertValue> columnEntry : columnFamilyEntry.getValue().entrySet()) {
		ColumnDefinition<?> columnDefinition = tableDefinition.getColumnDefinition(columnEntry.getKey());
		if (columnDefinition != null) {
		    if (tableDefinition.isPrimaryKey(columnDefinition)) {
			continue;
		    }
		    ColumnTypeDefinition<?> type = columnDefinition.getType();
		    byte[] value = type.toBytes(columnEntry.getValue());
		    put.addColumn(Bytes.toBytes(columnFamilyEntry.getKey()), Bytes.toBytes(columnEntry.getKey()),
			    value);
		} else {
		    put.addColumn(Bytes.toBytes(columnFamilyEntry.getKey()), Bytes.toBytes(columnEntry.getKey()),
			    (byte[]) columnEntry.getValue().getValue());
		}
	    }
	}
	tableEngine.put(put);
	return null;
    }

}
