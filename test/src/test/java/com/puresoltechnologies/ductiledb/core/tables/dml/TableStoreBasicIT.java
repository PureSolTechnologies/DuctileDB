package com.puresoltechnologies.ductiledb.core.tables.dml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
	createNamespace.bind().execute(tableStore);

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
	final String TABLE = "testSingleValueCrud";
	final String CF = "testcf";

	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable(NAMESPACE, TABLE);
	createTable.addColumn(CF, "static", ColumnType.INTEGER);
	createTable.addColumn(CF, "dynamic", ColumnType.VARCHAR);
	createTable.setPrimaryKey("static");
	createTable.bind().execute(tableStore);

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

    @Test
    public void testSelectSelection() throws ExecutionException, IOException {
	final String TABLE = "testSelectSelection";
	final String CF = "testcf";

	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable(NAMESPACE, TABLE);
	createTable.addColumn(CF, "static", ColumnType.INTEGER);
	createTable.addColumn(CF, "dynamic", ColumnType.VARCHAR);
	createTable.setPrimaryKey("static");
	createTable.bind().execute(tableStore);

	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();

	PreparedInsert insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 1);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("A").execute(tableStore);

	insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 2);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("B").execute(tableStore);

	insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 3);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("C").execute(tableStore);

	consoleOutput.printTableContent(tableStore, NAMESPACE, TABLE);

	PreparedSelect select = dml.prepareSelect(NAMESPACE, TABLE);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertEquals(1, (int) row.get("static"));
	    assertEquals("A", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(2, (int) row.get("static"));
	    assertEquals("B", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(3, (int) row.get("static"));
	    assertEquals("C", row.get("dynamic"));
	    assertFalse(iterator.hasNext());
	}

	select = dml.prepareSelect(NAMESPACE, TABLE);
	select.addWhereSelection("static", CompareOperator.EQUALS, 2);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertEquals(2, (int) row.get("static"));
	    assertEquals("B", row.get("dynamic"));
	    assertFalse(iterator.hasNext());
	}
    }

    @Test
    public void testPartAndAliasSelection() throws ExecutionException, IOException {
	final String TABLE = "testPartAndAliasSelection";
	final String CF = "testcf";

	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable(NAMESPACE, TABLE);
	createTable.addColumn(CF, "static", ColumnType.INTEGER);
	createTable.addColumn(CF, "dynamic", ColumnType.VARCHAR);
	createTable.setPrimaryKey("static");
	createTable.bind().execute(tableStore);

	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();

	PreparedInsert insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 1);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("A").execute(tableStore);

	insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 2);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("B").execute(tableStore);

	insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 3);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("C").execute(tableStore);

	consoleOutput.printTableContent(tableStore, NAMESPACE, TABLE);

	PreparedSelect select = dml.prepareSelect(NAMESPACE, TABLE);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertEquals(1, (int) row.get("static"));
	    assertEquals("A", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(2, (int) row.get("static"));
	    assertEquals("B", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(3, (int) row.get("static"));
	    assertEquals("C", row.get("dynamic"));
	    assertFalse(iterator.hasNext());
	}

	select = dml.prepareSelect(NAMESPACE, TABLE);
	select.selectColumn("static", "alias");
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertNull(row.get("static"));
	    assertNull(row.get("dynamic"));
	    assertEquals(1, (int) row.get("alias"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertNull(row.get("static"));
	    assertNull(row.get("dynamic"));
	    assertEquals(2, (int) row.get("alias"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertNull(row.get("static"));
	    assertNull(row.get("dynamic"));
	    assertEquals(3, (int) row.get("alias"));
	    assertFalse(iterator.hasNext());
	}
    }

    @Test
    public void testDeleteSelection() throws ExecutionException, IOException {
	final String TABLE = "testDeleteSelection";
	final String CF = "testcf";

	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable(NAMESPACE, TABLE);
	createTable.addColumn(CF, "static", ColumnType.INTEGER);
	createTable.addColumn(CF, "dynamic", ColumnType.VARCHAR);
	createTable.setPrimaryKey("static");
	createTable.bind().execute(tableStore);

	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();

	PreparedInsert insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 1);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("A").execute(tableStore);

	insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 2);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("B").execute(tableStore);

	insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 3);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("C").execute(tableStore);

	consoleOutput.printTableContent(tableStore, NAMESPACE, TABLE);

	PreparedSelect select = dml.prepareSelect(NAMESPACE, TABLE);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertEquals(1, (int) row.get("static"));
	    assertEquals("A", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(2, (int) row.get("static"));
	    assertEquals("B", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(3, (int) row.get("static"));
	    assertEquals("C", row.get("dynamic"));
	    assertFalse(iterator.hasNext());
	}

	PreparedDelete delete = dml.prepareDelete(NAMESPACE, TABLE);
	delete.addWhereSelection("static", CompareOperator.EQUALS, 2);
	delete.bind().execute(tableStore);

	select = dml.prepareSelect(NAMESPACE, TABLE);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertEquals(1, (int) row.get("static"));
	    assertEquals("A", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(3, (int) row.get("static"));
	    assertEquals("C", row.get("dynamic"));
	    assertFalse(iterator.hasNext());
	}
    }

    @Test
    public void testUdateSelection() throws ExecutionException, IOException {
	final String TABLE = "testUdateSelection";
	final String CF = "testcf";

	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable(NAMESPACE, TABLE);
	createTable.addColumn(CF, "static", ColumnType.INTEGER);
	createTable.addColumn(CF, "dynamic", ColumnType.VARCHAR);
	createTable.setPrimaryKey("static");
	createTable.bind().execute(tableStore);

	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();

	PreparedInsert insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 1);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("A").execute(tableStore);

	insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 2);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("B").execute(tableStore);

	insert = dml.prepareInsert(NAMESPACE, TABLE);
	insert.addValue(CF, "static", 3);
	insert.addPlaceholder(new Placeholder(1, CF, "dynamic"));
	insert.bind("C").execute(tableStore);

	consoleOutput.printTableContent(tableStore, NAMESPACE, TABLE);

	PreparedSelect select = dml.prepareSelect(NAMESPACE, TABLE);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertEquals(1, (int) row.get("static"));
	    assertEquals("A", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(2, (int) row.get("static"));
	    assertEquals("B", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(3, (int) row.get("static"));
	    assertEquals("C", row.get("dynamic"));
	    assertFalse(iterator.hasNext());
	}

	PreparedUpdate update = dml.prepareUpdate(NAMESPACE, TABLE);
	update.addWhereSelection("static", CompareOperator.EQUALS, 2);
	update.addValue(CF, "dynamic", "BB");
	update.bind().execute(tableStore);

	select = dml.prepareSelect(NAMESPACE, TABLE);
	try (TableRowIterable result = select.bind().execute(tableStore)) {
	    Iterator<TableRow> iterator = result.iterator();
	    assertTrue(iterator.hasNext());
	    TableRow row = iterator.next();
	    assertEquals(1, (int) row.get("static"));
	    assertEquals("A", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(2, (int) row.get("static"));
	    assertEquals("BB", row.get("dynamic"));
	    assertTrue(iterator.hasNext());
	    row = iterator.next();
	    assertEquals(3, (int) row.get("static"));
	    assertEquals("C", row.get("dynamic"));
	    assertFalse(iterator.hasNext());
	}
    }

}
