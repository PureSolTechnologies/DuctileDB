package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.io.File;

import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateIndex;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropIndex;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropTable;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class DataDefinitionLanguageImpl implements DataDefinitionLanguage {

    private final DatabaseEngineImpl storageEngine;
    private final File directory;

    public DataDefinitionLanguageImpl(DatabaseEngineImpl storageEngine, File directory) {
	this.storageEngine = storageEngine;
	this.directory = directory;
    }

    @Override
    public CreateNamespace createCreateNamespace(String namespace) {
	return new CreateNamespaceImpl(storageEngine, new File(directory, namespace));
    }

    @Override
    public DropNamespace createDropNamespace(String namespace) {
	return new DropNamespaceImpl(storageEngine, namespace);
    }

    @Override
    public CreateTable createCreateTable(String namespace, String table) {
	return new CreateTableImpl(storageEngine, namespace, table);
    }

    @Override
    public DropTable createDropTable(String namespace, String table) {
	return new DropTableImpl(storageEngine, namespace, table);
    }

    @Override
    public CreateIndex createCreateIndex(String namespace, String table, String index) {
	return new CreateIndexImpl(storageEngine, namespace, table, index);
    }

    @Override
    public DropIndex createDropIndex(String namespace, String table, String index) {
	return new DropIndexImpl(storageEngine, namespace, table, index);
    }
}
