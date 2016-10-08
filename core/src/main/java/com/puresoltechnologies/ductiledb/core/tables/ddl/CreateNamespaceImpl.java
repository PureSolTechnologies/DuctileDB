package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.io.File;

import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;

public class CreateNamespaceImpl implements CreateNamespace {

    private final TableStoreImpl tableStore;
    private final File namespaceDirectory;

    public CreateNamespaceImpl(TableStoreImpl storageEngine, File namespaceDirectory) {
	this.tableStore = storageEngine;
	this.namespaceDirectory = namespaceDirectory;
    }

    @Override
    public void execute() {
	DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	storageEngine
		.addNamespace(new NamespaceDescriptor(storageEngine.getStorage(), namespaceDirectory));
    }

}
