package com.puresoltechnologies.ductiledb.core.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.api.tables.ValueTypes;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropTable;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class TableStoreTablesIT extends AbstractTableStoreTest {

    private static NamespaceEngineImpl namespaceEngine;

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
	CreateNamespace createNamespace = ddl.createCreateNamespace("testnamespace");
	createNamespace.execute();

	namespaces = schemaManager.getNamespaces();
	namespaceIterator = namespaces.iterator();
	assertTrue(namespaceIterator.hasNext());
	assertEquals("system", namespaceIterator.next().getName());
	assertTrue(namespaceIterator.hasNext());
	assertEquals("testnamespace", namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());
	// TODO check also with TableStore API!!!

	namespaceEngine = storageEngine.getNamespaceEngine("testnamespace");
    }

    @Test
    public void testTableCrud() throws ExecutionException {
	TableStoreImpl tableStore = getTableStore();

	DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	SchemaManager schemaManager = storageEngine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.getNamespace("testnamespace");
	Iterable<TableDescriptor> tables = schemaManager.getTables(namespace);
	Iterator<TableDescriptor> tableIterator = tables.iterator();
	assertFalse(tableIterator.hasNext());

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable("testnamespace", "testtable");
	createTable.addColumn("testcf", "testcolumn", ValueTypes.VAR_CHAR);
	createTable.execute();

	tables = schemaManager.getTables(namespace);
	tableIterator = tables.iterator();
	assertTrue(tableIterator.hasNext());
	assertEquals("testtable", tableIterator.next().getName());
	assertFalse(tableIterator.hasNext());

	DropTable dropTable = ddl.createDropTable("testnamespace", "testtable");
	dropTable.execute();

	tables = schemaManager.getTables(namespace);
	tableIterator = tables.iterator();
	assertFalse(tableIterator.hasNext());
    }
}
