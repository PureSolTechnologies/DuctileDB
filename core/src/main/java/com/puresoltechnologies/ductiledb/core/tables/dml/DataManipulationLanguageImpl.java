package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;

public class DataManipulationLanguageImpl implements DataManipulationLanguage {

    private final TableStoreImpl tableStore;

    public DataManipulationLanguageImpl(TableStoreImpl tableStore) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public PreparedInsert prepareInsert(String namespace, String table) {
	TableDefinition tableDefinition = tableStore.getTableDefinition(namespace, table);
	return new PreparedInsertImpl(tableStore, tableDefinition);
    }

    @Override
    public PreparedUpdate prepareUpdate(String namespace, String table) {
	TableDefinition tableDefinition = tableStore.getTableDefinition(namespace, table);
	return new PreparedUpdateImpl(tableStore, tableDefinition);
    }

    @Override
    public PreparedDelete prepareDelete(String namespace, String table) {
	TableDefinition tableDefinition = tableStore.getTableDefinition(namespace, table);
	return new PreparedDeleteImpl(tableStore, tableDefinition);
    }

    @Override
    public PreparedSelect prepareSelect(String namespace, String table) {
	TableDefinition tableDefinition = tableStore.getTableDefinition(namespace, table);
	return new PreparedSelectImpl(tableStore, tableDefinition);
    }

    @Override
    public PreparedTruncate prepareTruncate(String namespace, String table) {
	TableDefinition tableDefinition = tableStore.getTableDefinition(namespace, table);
	return new PreparedTruncateImpl(tableStore, tableDefinition);
    }

}
