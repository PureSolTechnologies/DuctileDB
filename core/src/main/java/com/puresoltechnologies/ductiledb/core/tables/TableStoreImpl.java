package com.puresoltechnologies.ductiledb.core.tables;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.tables.TableStore;
import com.puresoltechnologies.ductiledb.api.tables.dcl.DataControlLanguage;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.dml.DataManipulationLanguage;
import com.puresoltechnologies.ductiledb.core.tables.dcl.DataControlLanguageImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguageImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinitionImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.DataManipulationLanguageImpl;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

/**
 * This is the central class for the RDBMS functionality of DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public class TableStoreImpl implements TableStore {

    private static Logger logger = LoggerFactory.getLogger(TableStoreImpl.class);

    public static String STORAGE_DIRECTORY = "tables";

    private final TableStoreConfiguration configuration;
    private final DatabaseEngineImpl storageEngine;
    private final boolean autoCloseConnection;
    private final TableStoreSchema tablesSchema;
    private final DataDefinitionLanguage dataDefinitionLanguage;
    private final DataManipulationLanguage dataManipulationLanguage;
    private final DataControlLanguage dataControlLanguage;

    public TableStoreImpl(TableStoreConfiguration configuration, DatabaseEngineImpl storageEngine,
	    boolean autoCloseConnection) throws StorageException, SchemaException {
	this.configuration = configuration;
	this.storageEngine = storageEngine;
	this.autoCloseConnection = autoCloseConnection;
	// Schema...
	this.tablesSchema = new TableStoreSchema(storageEngine, configuration);
	tablesSchema.checkAndCreateEnvironment();
	// Languages...
	this.dataDefinitionLanguage = new DataDefinitionLanguageImpl(this, new File(STORAGE_DIRECTORY));
	this.dataManipulationLanguage = new DataManipulationLanguageImpl(this);
	this.dataControlLanguage = new DataControlLanguageImpl(this);
    }

    @Override
    public void close() throws IOException {
	if (autoCloseConnection) {
	    if (!storageEngine.isClosed()) {
		logger.info("Closes connection '" + storageEngine.toString() + "'...");
		storageEngine.close();
		logger.info("Connection '" + storageEngine.toString() + "' closed.");
	    }
	}
    }

    public DatabaseEngineImpl getStorageEngine() {
	return storageEngine;
    }

    @Override
    public DataDefinitionLanguage getDataDefinitionLanguage() {
	return dataDefinitionLanguage;
    }

    @Override
    public DataManipulationLanguage getDataManipulationLanguage() {
	return dataManipulationLanguage;
    }

    @Override
    public DataControlLanguage getDataControlLanguage() {
	return dataControlLanguage;
    }

    public void runCompaction() {
	storageEngine.runCompaction();
    }

    public TableDefinitionImpl getTableDefinition(String namespace, String table) {
	// TODO Auto-generated method stub
	return null;
    }

}
