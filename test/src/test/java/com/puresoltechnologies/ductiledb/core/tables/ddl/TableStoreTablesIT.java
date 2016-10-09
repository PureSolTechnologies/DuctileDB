package com.puresoltechnologies.ductiledb.core.tables.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.api.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropTable;
import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.VarCharColumnType;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class TableStoreTablesIT extends AbstractTableStoreTest {

    private static NamespaceEngineImpl namespaceEngine;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void createTestnamespace() throws ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	SchemaManager schemaManager = storageEngine.getSchemaManager();

	// Check that only system namespace is available.
	Iterable<NamespaceDescriptor> namespaces = schemaManager.getNamespaces();
	Iterator<NamespaceDescriptor> namespaceIterator = namespaces.iterator();
	assertTrue(namespaceIterator.hasNext());
	assertEquals("system", namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());
	// TODO check also with TableStore API!!!

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateNamespace createNamespace = ddl.createCreateNamespace("tablesit");
	createNamespace.execute();

	namespaces = schemaManager.getNamespaces();
	namespaceIterator = namespaces.iterator();
	assertTrue(namespaceIterator.hasNext());
	assertEquals("tablesit", namespaceIterator.next().getName());
	assertTrue(namespaceIterator.hasNext());
	assertEquals("system", namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());
	// TODO check also with TableStore API!!!

	namespaceEngine = storageEngine.getNamespaceEngine("tablesit");
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

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable("tablesit", "testtable");
	createTable.addColumn("testcf", "testcolumn", new VarCharColumnType());
	createTable.execute();

	tables = schemaManager.getTables(namespace);
	tableIterator = tables.iterator();
	assertTrue(tableIterator.hasNext());
	assertEquals("testtable", tableIterator.next().getName());
	assertFalse(tableIterator.hasNext());

	DropTable dropTable = ddl.createDropTable("tablesit", "testtable");
	dropTable.execute();

	tables = schemaManager.getTables(namespace);
	tableIterator = tables.iterator();
	assertFalse(tableIterator.hasNext());
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
