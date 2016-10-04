package com.puresoltechnologies.ductiledb.core.rdbms.schema;

import com.puresoltechnologies.ductiledb.core.rdbms.DuctileDBRdbmsConfiguration;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

/**
 * This class creates the schema within the {@link DatabaseEngine} for RDBMS
 * part of DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public class RdbmsSchema {

    public static final String SYSTEM_NAMESPACE_NAME = "system";
    public static final String TABLES_TABLE_NAME = "tables";

    private final DatabaseEngineImpl storageEngine;
    private final DuctileDBRdbmsConfiguration configuration;

    public RdbmsSchema(DatabaseEngineImpl storageEngine, DuctileDBRdbmsConfiguration configuration) {
	this.storageEngine = storageEngine;
	this.configuration = configuration;
    }

    public void checkAndCreateEnvironment() throws StorageException, SchemaException {
	SchemaManager schemaManager = storageEngine.getSchemaManager();
	NamespaceDescriptor namespace = assureSystemNamespacePresence(schemaManager);
	assureNamespacesTablePresence(schemaManager, namespace);
	assureTablesTablePresence(schemaManager, namespace);
    }

    private NamespaceDescriptor assureSystemNamespacePresence(SchemaManager schemaManager)
	    throws StorageException, SchemaException {
	NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(SYSTEM_NAMESPACE_NAME);
	if (namespaceDescriptor == null) {
	    namespaceDescriptor = schemaManager.createNamespace(SYSTEM_NAMESPACE_NAME);
	}
	return namespaceDescriptor;
    }

    private void assureNamespacesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws StorageException, SchemaException {
	if (schemaManager.getTable(namespace, DatabaseTable.NAMESPACES.getName()) == null) {
	    TableDescriptor tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.NAMESPACES.getName());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getNameBytes());
	}
    }

    private void assureTablesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws StorageException, SchemaException {
	if (schemaManager.getTable(namespace, DatabaseTable.TABLES.getName()) == null) {
	    TableDescriptor tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.TABLES.getName());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getNameBytes());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.COLUMNS.getNameBytes());
	}
    }

}