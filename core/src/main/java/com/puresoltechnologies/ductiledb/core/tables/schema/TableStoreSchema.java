package com.puresoltechnologies.ductiledb.core.tables.schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.api.tables.ddl.NamespaceDefinition;
import com.puresoltechnologies.ductiledb.api.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreConfiguration;
import com.puresoltechnologies.ductiledb.core.tables.ddl.NamespaceDefinitionImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinitionImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.CompoundKey;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngine;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
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
public class TableStoreSchema {

    public static final String SYSTEM_NAMESPACE_NAME = "system";
    public static final byte[] SYSTEM_NAMESPACE_NAME_BYTES = Bytes.toBytes(SYSTEM_NAMESPACE_NAME);

    private final Map<String, NamespaceDefinition> namespaceDefinitions = new HashMap<>();
    private final Map<String, Map<String, TableDefinition>> tableDefinitions = new HashMap<>();

    private final DatabaseEngineImpl storageEngine;
    private final TableStoreConfiguration configuration;

    public TableStoreSchema(DatabaseEngineImpl storageEngine, TableStoreConfiguration configuration) {
	this.storageEngine = storageEngine;
	this.configuration = configuration;
    }

    public void checkAndCreateEnvironment() throws StorageException, SchemaException {
	SchemaManager schemaManager = storageEngine.getSchemaManager();
	NamespaceDescriptor namespace = assureSystemNamespacePresence(schemaManager);
	assureNamespacesTablePresence(schemaManager, namespace);
	assureTablesTablePresence(schemaManager, namespace);
	assureColumnsTablePresence(schemaManager, namespace);
	assureIndexesTablePresence(schemaManager, namespace);
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
	TableDescriptor tableDescriptor = schemaManager.getTable(namespace, DatabaseTable.NAMESPACES.getName());
	if (tableDescriptor == null) {
	    tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.NAMESPACES.getName());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getNameBytes());
	    TableEngine table = storageEngine.getTable(namespace.getName(), tableDescriptor.getName());

	    Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES).getKey());
	    table.put(put);
	}
    }

    private void assureTablesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws StorageException, SchemaException {
	TableDescriptor tableDescriptor = schemaManager.getTable(namespace, DatabaseTable.TABLES.getName());
	if (tableDescriptor == null) {
	    tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.TABLES.getName());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getNameBytes());
	    TableEngine table = storageEngine.getTable(namespace.getName(), tableDescriptor.getName());

	    Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.NAMESPACES.getNameBytes()).getKey());
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.TABLES.getNameBytes()).getKey());
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.COLUMNS.getNameBytes()).getKey());
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.INDEXES.getNameBytes()).getKey());
	    table.put(put);
	}
    }

    private void assureColumnsTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws StorageException, SchemaException {
	TableDescriptor tableDescriptor = schemaManager.getTable(namespace, DatabaseTable.COLUMNS.getName());
	if (tableDescriptor == null) {
	    tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.COLUMNS.getName());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getNameBytes());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.DEFINITION.getNameBytes());
	    TableEngine table = storageEngine.getTable(namespace.getName(), tableDescriptor.getName());

	    Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.NAMESPACES.getNameBytes()).getKey());
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.TABLES.getNameBytes()).getKey());
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.COLUMNS.getNameBytes()).getKey());
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.INDEXES.getNameBytes()).getKey());
	    table.put(put);
	}
    }

    private void assureIndexesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws StorageException, SchemaException {
	if (schemaManager.getTable(namespace, DatabaseTable.INDEXES.getName()) == null) {
	    TableDescriptor tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.INDEXES.getName());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getNameBytes());
	}
    }

    public void readDefinitions() {
	readNamespaceDefinitions();
	readTableDefinitions();
    }

    private void readNamespaceDefinitions() {
	TableEngine namespaceTable = storageEngine.getTable(TableStoreSchema.SYSTEM_NAMESPACE_NAME,
		DatabaseTable.NAMESPACES.getName());
	try (ResultScanner scanner = namespaceTable.getScanner(new Scan())) {
	    for (Result result : scanner) {
		byte[] rowKey = result.getRowKey();
		CompoundKey compoundKey = CompoundKey.of(rowKey);
		String namespaceName = Bytes.toString(compoundKey.getPart(0));
		NamespaceDefinitionImpl namespaceDefinition = new NamespaceDefinitionImpl(namespaceName);
		namespaceDefinitions.put(namespaceName, namespaceDefinition);
		tableDefinitions.put(namespaceName, new HashMap<>());
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not read definitions.", e);
	}
    }

    private void readTableDefinitions() {
	TableEngine tableTable = storageEngine.getTable(TableStoreSchema.SYSTEM_NAMESPACE_NAME,
		DatabaseTable.TABLES.getName());
	TableEngine columnsTable = storageEngine.getTable(TableStoreSchema.SYSTEM_NAMESPACE_NAME,
		DatabaseTable.COLUMNS.getName());
	try (ResultScanner scanner = tableTable.getScanner(new Scan())) {
	    for (Result result : scanner) {
		byte[] rowKey = result.getRowKey();
		CompoundKey compoundKey = CompoundKey.of(rowKey);
		String namespaceName = Bytes.toString(compoundKey.getPart(0));
		String tableName = Bytes.toString(compoundKey.getPart(1));
		TableDefinitionImpl tableDefinition = new TableDefinitionImpl(namespaceName, tableName);
		tableDefinitions.get(namespaceName).put(tableName, tableDefinition);
		rowKey[0] = 3;
		try (ResultScanner columnScanner = columnsTable.getScanner(new Scan(rowKey, rowKey))) {
		    for (Result columnResult : columnScanner) {
			byte[] columnRowKey = columnResult.getRowKey();
			CompoundKey columnCompoundKey = CompoundKey.of(columnRowKey);
			String columnName = Bytes.toString(columnCompoundKey.getPart(2));
			NavigableMap<byte[], byte[]> familyMap = columnResult
				.getFamilyMap(DatabaseColumnFamily.DEFINITION.getNameBytes());
			byte[] columnFamily = familyMap.get(DatabaseColumns.COLUMN_FAMILY.getNameBytes());
			byte[] type = familyMap.get(DatabaseColumns.TYPE.getNameBytes());
			// tableDefinition.addColumn(Bytes.toString(columnFamily),
			// columnName, type);
		    }
		}
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not read definitions.", e);
	}
    }

}
