package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class DataManipulationLanguageImpl implements DataManipulationLanguage {

    private final TableStoreImpl tableStore;

    public DataManipulationLanguageImpl(TableStoreImpl tableStore) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public PreparedInsert prepareInsert(String namespace, String table) {
	return new PreparedInsertImpl(tableStore, namespace, table);
    }

    @Override
    public PreparedUpdate prepareUpdate(String namespace, String table) {
	return new PreparedUpdateImpl(tableStore, namespace, table);
    }

    @Override
    public PreparedDelete prepareDelete(String namespace, String table) {
	return new PreparedDeleteImpl(tableStore, namespace, table);
    }

    @Override
    public PreparedSelect prepareSelect(String namespace, String table) {
	return new PreparedSelectImpl(tableStore, namespace, table);
    }

    @Override
    public PreparedTruncate prepareTruncate(String namespace, String table) {
	return new PreparedTruncateImpl(tableStore, namespace, table);
    }

}
