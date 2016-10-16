package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class DataManipulationLanguageImpl implements DataManipulationLanguage {

    private final TableStoreImpl tableStore;

    public DataManipulationLanguageImpl(TableStoreImpl tableStore) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public PreparedInsert preparedInsert(String namespace, String table) {
	return new PreparedInsertImpl(tableStore, namespace, table);
    }

    @Override
    public PreparedUpdate preparedUpdate(String namespace, String table) {
	return new PreparedUpdateImpl(tableStore, namespace, table);
    }

    @Override
    public PreparedDelete preparedDelete(String namespace, String table) {
	return new PreparedDeleteImpl(tableStore, namespace, table);
    }

    @Override
    public PreparedSelect preparedSelect(String namespace, String table) {
	return new PreparedSelectImpl(tableStore, namespace, table);
    }

    @Override
    public PreparedTruncate preparedTruncate(String namespace, String table) {
	return new PreparedTruncateImpl(tableStore, namespace, table);
    }

}
