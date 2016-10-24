package com.puresoltechnologies.ductiledb.core.tables.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.core.cli.DuctileDBConsoleOutput;
import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.core.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.core.tables.ddl.NamespaceDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;

public class TableStoreSchemaIT extends AbstractTableStoreTest {

    private static final DuctileDBConsoleOutput output = new DuctileDBConsoleOutput(System.out);

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testSystemNamespacesTable() throws IOException, ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	output.printTableContent(tableStore, "system", "namespaces");
    }

    @Test
    public void testSystemTablesTable() throws IOException, ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	output.printTableContent(tableStore, "system", "tables");
    }

    @Test
    public void testSystemColumnsTable() throws IOException, ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	output.printTableContent(tableStore, "system", "columns");
    }

    @Test
    public void testSchemaSurvivesRestart() throws ExecutionException {
	TableStore tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	final String NAMESPACE = "testSchemaSurvivesRestart";
	final String TABLE = "table1";
	CreateNamespace createNamespace = ddl.createCreateNamespace(NAMESPACE);
	createNamespace.execute(tableStore);
	CreateTable createTable = ddl.createCreateTable(NAMESPACE, TABLE);
	int id = 0;
	for (ColumnType type : ColumnType.values()) {
	    ++id;
	    createTable.addColumn("cf", "col" + id, type);
	}
	createTable.setPrimaryKey("col2", "col3");
	createTable.execute(tableStore);

	stopDatabase();
	startDatabase();

	tableStore = getTableStore();
	ddl = tableStore.getDataDefinitionLanguage();
	NamespaceDefinition namespaceDefinition = ddl.getNamespace(NAMESPACE);
	assertNotNull(namespaceDefinition);
	assertEquals(NAMESPACE, namespaceDefinition.getName());

	TableDefinition tableDefinition = ddl.getTable(NAMESPACE, TABLE);
	assertNotNull(tableDefinition);
	assertEquals(NAMESPACE, tableDefinition.getNamespace());
	assertEquals(TABLE, tableDefinition.getName());
	id = 0;
	for (ColumnType type : ColumnType.values()) {
	    ++id;
	    ColumnDefinition<?> columnDefinition = tableDefinition.getColumnDefinition("col" + id);
	    assertNotNull(columnDefinition);
	    assertEquals("cf", columnDefinition.getColumnFamily());
	    assertEquals("col" + id, columnDefinition.getName());
	    assertEquals(type.name(), columnDefinition.getType().getName());
	    if ((id == 2) || (id == 3)) {
		assertTrue(tableDefinition.isPrimaryKey(columnDefinition));
	    } else {
		assertFalse(tableDefinition.isPrimaryKey(columnDefinition));
	    }
	}
    }
}
