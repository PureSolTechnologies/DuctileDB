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

import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypeDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.core.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;

public class TableStoreBasicIT extends AbstractTableStoreTest {

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
	CreateNamespace createNamespace = ddl.createCreateNamespace("basicit");
	createNamespace.execute();

	namespaces = schemaManager.getNamespaces();
	namespaceIterator = namespaces.iterator();
	Set<String> namespaceSet = new HashSet<>();
	assertTrue(namespaceIterator.hasNext());
	namespaceSet.add(namespaceIterator.next().getName());
	assertTrue(namespaceIterator.hasNext());
	namespaceSet.add(namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());
	// TODO check also with TableStore API!!!
	assertTrue(namespaceSet.contains("basicit"));
	assertTrue(namespaceSet.contains("system"));

	namespaceEngine = storageEngine.getNamespaceEngine("basicit");
    }

    @Test
    public void testSystemNamespacesTable() throws IOException {
	TableStoreImpl tableStore = getTableStore();

	String namespace = "system";
	String table = "namespaces";

	printTableContent(tableStore, namespace, table);
    }

    @Test
    public void testSystemTablesTable() throws IOException {
	TableStoreImpl tableStore = getTableStore();

	String namespace = "system";
	String table = "tables";

	printTableContent(tableStore, namespace, table);
    }

    @Test
    public void testSystemColumnsTable() throws IOException {
	TableStoreImpl tableStore = getTableStore();

	String namespace = "system";
	String table = "columns";

	printTableContent(tableStore, namespace, table);
    }

    private void printTableContent(TableStoreImpl tableStore, String namespace, String table) throws IOException {
	StringBuilder builder = new StringBuilder();
	TableDefinition tableDefinition = tableStore.getTableDefinition(namespace, table);
	builder.append("---------------------------------------------------------\n");
	builder.append("TABLE: " + tableDefinition.getNamespace() + "." + tableDefinition.getName() + "\n");
	for (ColumnDefinition<?> columnDefinition : tableDefinition.getColumnDefinitions()) {
	    builder.append(columnDefinition.getName() + "\t");
	}
	builder.append("\n");
	builder.append("---------------------------------------------------------\n");
	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	Select select = dml.createSelect(namespace, table);
	try (TableRowIterable tableRows = select.execute()) {
	    int count = 0;
	    for (TableRow tableRow : tableRows) {
		for (ColumnDefinition<?> columnDefinition : tableDefinition.getColumnDefinitions()) {
		    ColumnTypeDefinition<?> type = columnDefinition.getType();
		    byte[] value = tableRow.getBytes(columnDefinition.getName());
		    builder.append(type.fromBytes(value) + "\t");
		}
		builder.append("\n");
		count++;
	    }
	    System.out.println(builder.toString());
	    assertTrue(count > 0);
	}
    }

    @Test
    public void testValueCrud() throws ExecutionException, IOException {
	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable("basicit", "valuecrud");
	createTable.addColumn("testcf", "testcolumn", ColumnType.VARCHAR);
	createTable.execute();

	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();

	Insert insert = dml.createInsert("basicit", "valuecrud");
	insert.addValue("testcf", "testcolumn", "teststring");
	insert.execute();

	printTableContent(tableStore, "basicit", "valuecrud");
    }
}
