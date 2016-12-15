package com.puresoltechnologies.ductiledb.core.tables.schema;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreConfiguration;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.ddl.NamespaceDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.NamespaceDefinitionImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinitionImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.CompoundKey;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngine;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.SecondaryIndexDescriptor;
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
    public static final Key SYSTEM_NAMESPACE_KEY = Key.of(SYSTEM_NAMESPACE_NAME);

    private final Map<String, NamespaceDefinition> namespaceDefinitions = new HashMap<>();
    private final Map<String, Map<String, TableDefinition>> tableDefinitions = new HashMap<>();
    private final Map<String, Map<String, SecondaryIndexDescriptor>> indexDefinitions = new HashMap<>();

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
	    tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.NAMESPACES.getName(),
		    "Contains all namespaces.");
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getKey());
	    TableEngine table = storageEngine.getTable(namespace.getName(), tableDescriptor.getName());

	    defineSystemNamespace(table);
	}
    }

    private void defineSystemNamespace(TableEngine table) {
	ColumnValue now = ColumnValue.of(Bytes.toBytes(Instant.now()));

	Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	table.put(put);
    }

    private void assureTablesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws StorageException, SchemaException {
	TableDescriptor tableDescriptor = schemaManager.getTable(namespace, DatabaseTable.TABLES.getName());
	if (tableDescriptor == null) {
	    tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.TABLES.getName(),
		    "Contains all tables.");
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getKey());
	    TableEngine table = storageEngine.getTable(namespace.getName(), tableDescriptor.getName());

	    defineSystemTables(table);
	}
    }

    private void defineSystemTables(TableEngine table) {
	ColumnValue now = ColumnValue.of(Bytes.toBytes(Instant.now()));

	Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, //
		DatabaseTable.NAMESPACES.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, //
		DatabaseTable.TABLES.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, //
		DatabaseTable.COLUMNS.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, //
		DatabaseTable.INDEXES.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	table.put(put);
    }

    private void assureColumnsTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws StorageException, SchemaException {
	TableDescriptor tableDescriptor = schemaManager.getTable(namespace, DatabaseTable.COLUMNS.getName());
	if (tableDescriptor == null) {
	    tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.COLUMNS.getName(),
		    "Contains all columns.");
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getKey());
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.DEFINITION.getKey());
	    TableEngine table = storageEngine.getTable(namespace.getName(), tableDescriptor.getName());

	    defineSystemTableColumns(table);
	}
    }

    private void defineSystemTableColumns(TableEngine table) {
	ColumnValue now = ColumnValue.of(Bytes.toBytes(Instant.now()));

	defineSystemNamespacesColumns(table, now);
	defineSystemTablesColumns(table, now);
	defineSystemColumnsColumns(table, now);
    }

    private void defineSystemNamespacesColumns(TableEngine table, ColumnValue now) {
	Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.NAMESPACES.getKey(),
		DatabaseColumns.NAMESPACE.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.ROWKEY.getNameBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.VARCHAR.name()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.PRIMARY_KEY_PART.getKey(),
		ColumnValue.of((byte) 0));
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.NAMESPACES.getKey(),
		DatabaseColumns.CREATED.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.ROWKEY.getNameBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.TIMESTAMP.name()));
	table.put(put);

    }

    private void defineSystemTablesColumns(TableEngine table, ColumnValue now) {
	Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.TABLES.getKey(),
		DatabaseColumns.NAMESPACE.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.ROWKEY.getNameBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.VARCHAR.name()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.PRIMARY_KEY_PART.getKey(),
		ColumnValue.of((byte) 0));
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.TABLES.getKey(),
		DatabaseColumns.TABLE.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.ROWKEY.getNameBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.VARCHAR.name()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.PRIMARY_KEY_PART.getKey(),
		ColumnValue.of((byte) 1));
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.TABLES.getKey(),
		DatabaseColumns.CREATED.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.ROWKEY.getNameBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.TIMESTAMP.name()));
	table.put(put);

    }

    private void defineSystemColumnsColumns(TableEngine table, ColumnValue now) {
	Put put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.COLUMNS.getKey(),
		DatabaseColumns.NAMESPACE.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.ROWKEY.getKey().getBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of((ColumnType.VARCHAR.name())));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.PRIMARY_KEY_PART.getKey(),
		ColumnValue.of(((byte) 0)));
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.COLUMNS.getKey(),
		DatabaseColumns.TABLE.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.ROWKEY.getKey().getBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of((ColumnType.VARCHAR.name())));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.PRIMARY_KEY_PART.getKey(),
		ColumnValue.of(((byte) 1)));
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.COLUMNS.getKey(),
		DatabaseColumns.COLUMN.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.ROWKEY.getNameBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.VARCHAR.name()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.PRIMARY_KEY_PART.getKey(),
		ColumnValue.of((byte) 2));
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.COLUMNS.getKey(),
		DatabaseColumns.CREATED.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.METADATA.getNameBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.TIMESTAMP.name()));
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.COLUMNS.getKey(),
		DatabaseColumns.COLUMN_FAMILY.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.DEFINITION.getNameBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.VARCHAR.name()));
	table.put(put);

	put = new Put(CompoundKey.create(SYSTEM_NAMESPACE_KEY, DatabaseTable.COLUMNS.getKey(),
		DatabaseColumns.TYPE.getKey()));
	put.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumns.CREATED.getKey(), now);
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.COLUMN_FAMILY.getKey(),
		ColumnValue.of(DatabaseColumnFamily.DEFINITION.getKey().getBytes()));
	put.addColumn(DatabaseColumnFamily.DEFINITION.getKey(), DatabaseColumns.TYPE.getKey(),
		ColumnValue.of(ColumnType.VARCHAR.name()));
	table.put(put);
    }

    private void assureIndexesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws StorageException, SchemaException {
	if (schemaManager.getTable(namespace, DatabaseTable.INDEXES.getName()) == null) {
	    TableDescriptor tableDescriptor = schemaManager.createTable(namespace, DatabaseTable.INDEXES.getName(),
		    "Contains all indizes.");
	    schemaManager.createColumnFamily(tableDescriptor, DatabaseColumnFamily.METADATA.getKey());
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
		Key rowKey = result.getRowKey();
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
		Key rowKey = result.getRowKey();
		CompoundKey compoundKey = CompoundKey.of(rowKey);
		String namespaceName = Bytes.toString(compoundKey.getPart(0));
		String tableName = Bytes.toString(compoundKey.getPart(1));
		ColumnMap metadata = result.getFamilyMap(Key.of("metadata"));
		ColumnValue description = metadata.get(Key.of("description"));
		TableDefinitionImpl tableDefinition = new TableDefinitionImpl(namespaceName, tableName,
			description != null ? description.toString() : "n/a");
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
		Key columnRowKey = columnResult.getRowKey();
		CompoundKey columnCompoundKey = CompoundKey.of(columnRowKey);
		String namespaceName = Bytes.toString(columnCompoundKey.getPart(0));
		String tableName = Bytes.toString(columnCompoundKey.getPart(1));
		String columnName = Bytes.toString(columnCompoundKey.getPart(2));
		NavigableMap<Key, ColumnValue> familyMap = columnResult
			.getFamilyMap(DatabaseColumnFamily.DEFINITION.getKey());
		String columnFamily = familyMap.get(DatabaseColumns.COLUMN_FAMILY.getKey()).toString();
		ColumnType type = ColumnType.valueOf(familyMap.get(DatabaseColumns.TYPE.getKey()).toString());
		ColumnValue primaryKeyPart = familyMap.get(DatabaseColumns.PRIMARY_KEY_PART.getKey());

		TableDefinitionImpl tableDefinition = (TableDefinitionImpl) tableDefinitions.get(namespaceName)
			.get(tableName);
		tableDefinition.addColumn(columnFamily, columnName, type);
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
		    tableMap.put(primaryKeyPart.toByte() & 0xFF, columnName);
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

    public Iterable<TableDefinition> getTableDefinitions(String namespace) {
	Map<String, TableDefinition> namespaceMap = tableDefinitions.get(namespace);
	return namespaceMap != null ? namespaceMap.values() : null;
    }

    public void addTableDefinition(String namespace, TableDefinitionImpl tableDefinition) {
	tableDefinitions.get(namespace).put(tableDefinition.getName(), tableDefinition);
    }

    public void removeTableDefinition(String namespace, String table) {
	tableDefinitions.get(namespace).remove(table);
    }

    public Iterable<NamespaceDefinition> getNamespaceDefinitions() {
	return namespaceDefinitions.values();
    }

    public NamespaceDefinition getNamespaceDefinition(String namespace) {
	return namespaceDefinitions.get(namespace);
    }

    public void addNamespaceDefinition(NamespaceDefinition namespaceDefinition) {
	namespaceDefinitions.put(namespaceDefinition.getName(), namespaceDefinition);
	tableDefinitions.put(namespaceDefinition.getName(), new HashMap<>());
	indexDefinitions.put(namespaceDefinition.getName(), new HashMap<>());
    }

    public void removeNamespaceDefinition(String name) {
	namespaceDefinitions.remove(name);
	tableDefinitions.remove(name);
	indexDefinitions.remove(name);
    }

    public void addIndexDefinition(String namespace, SecondaryIndexDescriptor indexDescriptor) {
	indexDefinitions.get(namespace).put(indexDescriptor.getName(), indexDescriptor);
    }

    public void removeIndexDefinition(String namespace, String index) {
	indexDefinitions.get(namespace).remove(index);
    }
}
