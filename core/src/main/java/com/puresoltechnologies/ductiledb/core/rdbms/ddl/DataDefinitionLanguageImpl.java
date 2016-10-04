package com.puresoltechnologies.ductiledb.core.rdbms.ddl;

import com.puresoltechnologies.ductiledb.api.rdbms.ddl.CreateIndex;
import com.puresoltechnologies.ductiledb.api.rdbms.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.api.rdbms.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.api.rdbms.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.rdbms.ddl.DropIndex;
import com.puresoltechnologies.ductiledb.api.rdbms.ddl.DropNamespace;
import com.puresoltechnologies.ductiledb.api.rdbms.ddl.DropTable;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class DataDefinitionLanguageImpl implements DataDefinitionLanguage {

    private final DatabaseEngineImpl storageEngine;

    public DataDefinitionLanguageImpl(DatabaseEngineImpl storageEngine) {
	this.storageEngine = storageEngine;
    }

    @Override
    public CreateNamespace createCreateNamespace(String namespace) {
	return new CreateNamespaceImpl(storageEngine, namespace);
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
