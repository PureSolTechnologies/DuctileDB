package com.puresoltechnologies.ductiledb.core.rdbms.ddl;

import com.puresoltechnologies.ductiledb.api.rdbms.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;

public class CreateNamespaceImpl implements CreateNamespace {

    private final DatabaseEngineImpl storageEngine;
    private final String namespace;

    public CreateNamespaceImpl(DatabaseEngineImpl storageEngine, String namespace) {
	this.storageEngine = storageEngine;
	this.namespace = namespace;
    }

    @Override
    public void execute() {
	storageEngine.addNamespace(new NamespaceDescriptor(storageEngine.getStorage(), namespace));
    }

}
