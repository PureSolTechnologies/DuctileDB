package com.puresoltechnologies.ductiledb.core.tables.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class TableStoreTablesIT extends AbstractTableStoreTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void createTestnamespace() throws ExecutionException {
	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateNamespace createNamespace = ddl.createCreateNamespace("tablesit");
	createNamespace.execute();

	TableStoreSchema schema = tableStore.getSchema();
	NamespaceDefinition namespaceDefinition = schema.getNamespaceDefinition("tablesit");
	assertNotNull(namespaceDefinition);
	assertEquals("tablesit", namespaceDefinition.getName());

    }

    @Test
    public void testTableCrud() throws ExecutionException {
	TableStoreImpl tableStore = getTableStore();

	DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	SchemaManager schemaManager = storageEngine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.getNamespace("tablesit");
	Iterable<TableDescriptor> tables = schemaManager.getTables(namespace);
	Iterator<TableDescriptor> tableIterator = tables.iterator();
	assertFalse(tableIterator.hasNext());

	TableStoreSchema schema = tableStore.getSchema();
	Iterable<TableDefinition> tableDefinitions = schema.getTableDefinitions("tablesit");
	assertNotNull(tableDefinitions);
	Iterator<TableDefinition> tableDefinitionIterator = tableDefinitions.iterator();
	assertNotNull(tableDefinitionIterator);
	assertFalse(tableDefinitionIterator.hasNext());

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable("tablesit", "testtable");
	createTable.addColumn("testcf", "testcolumn", ColumnType.VARCHAR);
	createTable.execute();

	tables = schemaManager.getTables(namespace);
	tableIterator = tables.iterator();
	assertTrue(tableIterator.hasNext());
	assertEquals("testtable", tableIterator.next().getName());
	assertFalse(tableIterator.hasNext());

	schema = tableStore.getSchema();
	tableDefinitions = schema.getTableDefinitions("tablesit");
	assertNotNull(tableDefinitions);
	tableDefinitionIterator = tableDefinitions.iterator();
	assertNotNull(tableDefinitionIterator);
	assertTrue(tableDefinitionIterator.hasNext());
	assertEquals("testtable", tableDefinitionIterator.next().getName());
	assertFalse(tableDefinitionIterator.hasNext());

	DropTable dropTable = ddl.createDropTable("tablesit", "testtable");
	dropTable.execute();

	tables = schemaManager.getTables(namespace);
	tableIterator = tables.iterator();
	assertFalse(tableIterator.hasNext());

	schema = tableStore.getSchema();
	tableDefinitions = schema.getTableDefinitions("tablesit");
	assertNotNull(tableDefinitions);
	tableDefinitionIterator = tableDefinitions.iterator();
	assertNotNull(tableDefinitionIterator);
	assertFalse(tableDefinitionIterator.hasNext());
    }

    @Test
    public void testCreatingTablesInSystemNamespaceForbidden() throws ExecutionException {
	exception.expect(ExecutionException.class);
	exception.expectMessage("Creating tables in 'system' namespace is not allowed.");

	TableStoreImpl tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	ddl.createCreateTable("system", "testtable").execute();
    }

    @Test
    public void testDroppingTablesFromSystemNamespaceForbidden() throws ExecutionException {
	exception.expect(ExecutionException.class);
	exception.expectMessage("Dropping tables from 'system' namespace is not allowed.");

	TableStoreImpl tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	ddl.createDropTable("system", "namespaces").execute();
    }
}
