package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.io.File;

import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;

public class CreateNamespaceImpl implements CreateNamespace {

    private final DatabaseEngineImpl storageEngine;
    private final File namespaceDirectory;

    public CreateNamespaceImpl(DatabaseEngineImpl storageEngine, File namespaceDirectory) {
	this.storageEngine = storageEngine;
	this.namespaceDirectory = namespaceDirectory;
    }

    @Override
    public void execute() {
	storageEngine.addNamespace(new NamespaceDescriptor(storageEngine.getStorage(), namespaceDirectory));
    }

}
