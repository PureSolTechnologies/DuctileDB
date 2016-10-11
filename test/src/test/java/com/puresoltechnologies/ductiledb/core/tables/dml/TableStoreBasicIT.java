package com.puresoltechnologies.ductiledb.core.tables.dml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.api.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.dml.DataManipulationLanguage;
import com.puresoltechnologies.ductiledb.api.tables.dml.Insert;
import com.puresoltechnologies.ductiledb.api.tables.dml.Select;
import com.puresoltechnologies.ductiledb.api.tables.dml.TableRow;
import com.puresoltechnologies.ductiledb.api.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.VarCharColumnType;
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
    public void testSystemColumnsTable() {
	TableStoreImpl tableStore = getTableStore();

	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	Select select = dml.createSelect("system", "columns");
	TableRowIterable results = select.execute();
	int count = 0;
	for (TableRow result : results) {
	    count++;
	}
	assertTrue(count > 0);
    }

    @Test
    public void testValueCrud() throws ExecutionException {
	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable("basicit", "valuecrud");
	createTable.addColumn("testcf", "testcolumn", new VarCharColumnType());
	createTable.execute();

	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();

	Insert insert = dml.createInsert("basicit", "valuecrud");
	insert.addValue("testcf", "testcolumn", "teststring");
	insert.execute();
    }
}
