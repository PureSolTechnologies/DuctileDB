package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.api.tables.dml.DataManipulationLanguage;
import com.puresoltechnologies.ductiledb.api.tables.dml.Delete;
import com.puresoltechnologies.ductiledb.api.tables.dml.Insert;
import com.puresoltechnologies.ductiledb.api.tables.dml.Select;
import com.puresoltechnologies.ductiledb.api.tables.dml.Truncate;
import com.puresoltechnologies.ductiledb.api.tables.dml.Update;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class DataManipulationLanguageImpl implements DataManipulationLanguage {

    private final TableStoreImpl tableStore;

    public DataManipulationLanguageImpl(TableStoreImpl tableStore) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public Insert createInsert(String namespace, String table) {
	return new InsertImpl(tableStore, namespace, table);
    }

    @Override
    public Update createUpdate(String namespace, String table) {
	return new UpdateImpl(tableStore, namespace, table);
    }

    @Override
    public Delete createDelete(String namespace, String table) {
	return new DeleteImpl(tableStore, namespace, table);
    }

    @Override
    public Select createSelect(String namespace, String table) {
	return new SelectImpl(tableStore, namespace, table);
    }

    @Override
    public Truncate createTruncate(String namespace, String table) {
	return new TruncateImpl(tableStore, namespace, table);
    }

}
