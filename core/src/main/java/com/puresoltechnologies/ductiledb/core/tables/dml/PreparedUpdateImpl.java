package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnValue;

public class PreparedUpdateImpl extends AbstractPreparedWhereSelectionStatement implements PreparedUpdate {

    private final String namespace;
    private final String table;
    private final Map<String, Map<String, InsertValue>> values = new HashMap<>();

    public PreparedUpdateImpl(TableDefinition tableDefinition) {
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
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValues)
	    throws ExecutionException {
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	TableDefinition tableDefinition = ddl.getTable(getNamespace(), getTableName());
	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	PreparedSelect select = dml.prepareSelect(getNamespace(), getTableName());
	select.addWhereSelections(getSelections(placeholderValues));
	TableRowIterable rows = select.bind().execute(tableStore);
	TableEngineImpl tableEngine = getTableEngine(tableStore);
	for (TableRow row : rows) {
	    Put put = new Put(row.getRowKey());
	    for (Entry<String, Map<String, InsertValue>> columnFamilyValue : values.entrySet()) {
		Key columnFamilyKey = Key.of(columnFamilyValue.getKey());
		for (Entry<String, InsertValue> columnValue : columnFamilyValue.getValue().entrySet()) {
		    Key columnKey = Key.of(columnValue.getKey());
		    ColumnDefinition<?> columnDefinition = tableDefinition.getColumnDefinition(columnValue.getKey());
		    byte[] value = columnDefinition.getType().toBytes(columnValue.getValue().getValue());
		    put.addColumn(columnFamilyKey, columnKey, ColumnValue.of(value));
		}
	    }
	    tableEngine.put(put);
	}
	return null;
    }

}
