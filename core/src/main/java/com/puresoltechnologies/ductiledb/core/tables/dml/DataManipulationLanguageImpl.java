package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.api.tables.dml.DataManipulationLanguage;
import com.puresoltechnologies.ductiledb.api.tables.dml.Delete;
import com.puresoltechnologies.ductiledb.api.tables.dml.Insert;
import com.puresoltechnologies.ductiledb.api.tables.dml.Select;
import com.puresoltechnologies.ductiledb.api.tables.dml.Truncate;
import com.puresoltechnologies.ductiledb.api.tables.dml.Update;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class DataManipulationLanguageImpl implements DataManipulationLanguage {

    private final DatabaseEngineImpl databaseEngine;

    public DataManipulationLanguageImpl(DatabaseEngineImpl databaseEngine) {
	super();
	this.databaseEngine = databaseEngine;
    }

    @Override
    public Insert createInsert(String namespace, String table) {
	return new InsertImpl(databaseEngine, namespace, table);
    }

    @Override
    public Update createUpdate(String namespace, String table) {
	return new UpdateImpl(databaseEngine, namespace, table);
    }

    @Override
    public Delete createDelete(String namespace, String table) {
	return new DeleteImpl(databaseEngine, namespace, table);
    }

    @Override
    public Select createSelect(String namespace, String table) {
	return new SelectImpl(databaseEngine, namespace, table);
    }

    @Override
    public Truncate createTruncate(String namespace, String table) {
	return new TruncateImpl(databaseEngine, namespace, table);
    }

}
