package com.puresoltechnologies.ductiledb.core.tables.dml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.core.cli.DuctileDBConsoleOutput;
import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.core.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;

public class TableStoreBasicIT extends AbstractTableStoreTest {

    private static final String NAMESPACE = "basicit";
    private static final DuctileDBConsoleOutput consoleOutput = new DuctileDBConsoleOutput(System.out);

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
	CreateNamespace createNamespace = ddl.createCreateNamespace(NAMESPACE);
	createNamespace.execute(tableStore);

	namespaces = schemaManager.getNamespaces();
	namespaceIterator = namespaces.iterator();
	Set<String> namespaceSet = new HashSet<>();
	assertTrue(namespaceIterator.hasNext());
	namespaceSet.add(namespaceIterator.next().getName());
	assertTrue(namespaceIterator.hasNext());
	namespaceSet.add(namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());
	// TODO check also with TableStore API!!!
	assertTrue(namespaceSet.contains(NAMESPACE));
	assertTrue(namespaceSet.contains("system"));

	namespaceEngine = storageEngine.getNamespaceEngine(NAMESPACE);
    }

    @Test
    public void testSingleValueCrud() throws ExecutionException, IOException {
	final String CF = "testcf";
	final String TABLE = "valuecrud";

	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable(NAMESPACE, TABLE);
	createTable.addColumn(CF, "static", ColumnType.INTEGER);
	createTable.addColumn(CF, "dynamic", ColumnType.VARCHAR);
	createTable.setPrimaryKey("static");
	createTable.execute(tableStore);

	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();

	PreparedInsert insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 1);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("A").execute(tableStore);

	consoleOutput.printTableContent(tableStore, NAMESPACE, TABLE);

	PreparedSelect select = dml.prepareSelect(NAMESPACE, TABLE);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertEquals(1, (int) row.get("static"));
	    assertEquals("A", row.get("dynamic"));
	    assertFalse(iterator.hasNext());
	}

	PreparedDelete delete = dml.prepareDelete(NAMESPACE, TABLE);
	delete.bind().execute(tableStore);

	select = dml.prepareSelect(NAMESPACE, TABLE);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertFalse(iterator.hasNext());
	}
    }
}
