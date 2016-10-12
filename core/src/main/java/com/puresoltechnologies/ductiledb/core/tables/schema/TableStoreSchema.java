package com.puresoltechnologies.ductiledb.core.tables.schema;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.api.tables.ddl.NamespaceDefinition;
import com.puresoltechnologies.ductiledb.api.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreConfiguration;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypes;
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

	    byte[] now = Bytes.toBytes(Instant.now());

	    Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
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

	    byte[] now = Bytes.toBytes(Instant.now());

	    Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.NAMESPACES.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.TABLES.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.COLUMNS.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, //
		    DatabaseTable.INDEXES.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
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

	    byte[] now = Bytes.toBytes(Instant.now());

	    Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, DatabaseTable.COLUMNS.getNameBytes(),
		    DatabaseColumns.NAMESPACE.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.COLUMN_FAMILY.getNameBytes(),
		    DatabaseColumnFamily.ROWKEY.getNameBytes());
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.TYPE.getNameBytes(),
		    Bytes.toBytes(ColumnTypes.VARCHAR.name()));
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(),
		    DatabaseColumns.PRIMARY_KEY_PART.getNameBytes(), Bytes.toBytes((byte) 0));
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, DatabaseTable.COLUMNS.getNameBytes(),
		    DatabaseColumns.TABLE.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.COLUMN_FAMILY.getNameBytes(),
		    DatabaseColumnFamily.ROWKEY.getNameBytes());
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.TYPE.getNameBytes(),
		    Bytes.toBytes(ColumnTypes.VARCHAR.name()));
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(),
		    DatabaseColumns.PRIMARY_KEY_PART.getNameBytes(), Bytes.toBytes((byte) 1));
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, DatabaseTable.COLUMNS.getNameBytes(),
		    DatabaseColumns.COLUMN.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.COLUMN_FAMILY.getNameBytes(),
		    DatabaseColumnFamily.ROWKEY.getNameBytes());
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.TYPE.getNameBytes(),
		    Bytes.toBytes(ColumnTypes.VARCHAR.name()));
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(),
		    DatabaseColumns.PRIMARY_KEY_PART.getNameBytes(), Bytes.toBytes((byte) 2));
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, DatabaseTable.COLUMNS.getNameBytes(),
		    DatabaseColumns.CREATED.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.COLUMN_FAMILY.getNameBytes(),
		    DatabaseColumnFamily.METADATA.getNameBytes());
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.TYPE.getNameBytes(),
		    Bytes.toBytes(ColumnTypes.TIMESTAMP.name()));
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, DatabaseTable.COLUMNS.getNameBytes(),
		    DatabaseColumns.COLUMN_FAMILY.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.COLUMN_FAMILY.getNameBytes(),
		    DatabaseColumnFamily.DEFINITION.getNameBytes());
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.TYPE.getNameBytes(),
		    Bytes.toBytes(ColumnTypes.VARCHAR.name()));
	    table.put(put);

	    put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_NAME_BYTES, DatabaseTable.COLUMNS.getNameBytes(),
		    DatabaseColumns.TYPE.getNameBytes()).getKey());
	    put.addColumn(DatabaseColumnFamily.METADATA.getNameBytes(), DatabaseColumns.CREATED.getNameBytes(), now);
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.COLUMN_FAMILY.getNameBytes(),
		    DatabaseColumnFamily.DEFINITION.getNameBytes());
	    put.addColumn(DatabaseColumnFamily.DEFINITION.getNameBytes(), DatabaseColumns.TYPE.getNameBytes(),
		    Bytes.toBytes(ColumnTypes.VARCHAR.name()));
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
	try {
	    readNamespaceDefinitions();
	    readTableDefinitions();
	    readColumnDefinitions();
	} catch (IOException e) {
	    throw new StorageException("Could not read definitions.", e);
	}
    }

    private void readNamespaceDefinitions() throws IOException {
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
	}
    }

    private void readTableDefinitions() throws IOException {
	TableEngine tableTable = storageEngine.getTable(TableStoreSchema.SYSTEM_NAMESPACE_NAME,
		DatabaseTable.TABLES.getName());
	try (ResultScanner scanner = tableTable.getScanner(new Scan())) {
	    for (Result result : scanner) {
		byte[] rowKey = result.getRowKey();
		CompoundKey compoundKey = CompoundKey.of(rowKey);
		String namespaceName = Bytes.toString(compoundKey.getPart(0));
		String tableName = Bytes.toString(compoundKey.getPart(1));
		TableDefinitionImpl tableDefinition = new TableDefinitionImpl(namespaceName, tableName);
		tableDefinitions.get(namespaceName).put(tableName, tableDefinition);
	    }
	}
    }

    private void readColumnDefinitions() throws IOException {
	Map<String, Map<String, Map<Integer, String>>> primaryKeyParts = new HashMap<>();
	TableEngine columnsTable = storageEngine.getTable(TableStoreSchema.SYSTEM_NAMESPACE_NAME,
		DatabaseTable.COLUMNS.getName());
	try (ResultScanner columnScanner = columnsTable.getScanner(new Scan())) {
	    for (Result columnResult : columnScanner) {
		byte[] columnRowKey = columnResult.getRowKey();
		CompoundKey columnCompoundKey = CompoundKey.of(columnRowKey);
		String namespaceName = Bytes.toString(columnCompoundKey.getPart(0));
		String tableName = Bytes.toString(columnCompoundKey.getPart(1));
		String columnName = Bytes.toString(columnCompoundKey.getPart(2));
		NavigableMap<byte[], byte[]> familyMap = columnResult
			.getFamilyMap(DatabaseColumnFamily.DEFINITION.getNameBytes());
		String columnFamily = Bytes.toString(familyMap.get(DatabaseColumns.COLUMN_FAMILY.getNameBytes()));
		ColumnTypes type = ColumnTypes
			.valueOf(Bytes.toString(familyMap.get(DatabaseColumns.TYPE.getNameBytes())));
		byte[] primaryKeyPart = familyMap.get(DatabaseColumns.PRIMARY_KEY_PART.getNameBytes());

		TableDefinitionImpl tableDefinition = (TableDefinitionImpl) tableDefinitions.get(namespaceName)
			.get(tableName);
		tableDefinition.addColumn(columnFamily, columnName, type.getType());
		if (primaryKeyPart != null) {
		    Map<String, Map<Integer, String>> namespaceMap = primaryKeyParts.get(namespaceName);
		    if (namespaceMap == null) {
			namespaceMap = new HashMap<>();
			primaryKeyParts.put(namespaceName, namespaceMap);
		    }
		    Map<Integer, String> tableMap = namespaceMap.get(tableName);
		    if (tableMap == null) {
			tableMap = new HashMap<>();
			namespaceMap.put(tableName, tableMap);
		    }
		    tableMap.put(Bytes.toByte(primaryKeyPart) & 0xFF, columnName);
		}
	    }
	    for (Entry<String, Map<String, Map<Integer, String>>> namespaceEntry : primaryKeyParts.entrySet()) {
		String namespaceName = namespaceEntry.getKey();
		for (Entry<String, Map<Integer, String>> tableEntry : namespaceEntry.getValue().entrySet()) {
		    String tableName = tableEntry.getKey();
		    String[] keyParts = new String[tableEntry.getValue().size()];
		    for (Entry<Integer, String> columnEntry : tableEntry.getValue().entrySet()) {
			keyParts[columnEntry.getKey() & 0xFF] = columnEntry.getValue();
		    }
		    TableDefinitionImpl tableDefinition = (TableDefinitionImpl) tableDefinitions.get(namespaceName)
			    .get(tableName);
		    tableDefinition.setPrimaryKey(keyParts);
		}
	    }
	}
    }

    public TableDefinition getTableDefinition(String namespace, String table) {
	Map<String, TableDefinition> definitions = tableDefinitions.get(namespace);
	return definitions != null ? definitions.get(table) : null;
    }
}
