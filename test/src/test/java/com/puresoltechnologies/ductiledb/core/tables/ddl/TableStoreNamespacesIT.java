package com.puresoltechnologies.ductiledb.core.tables.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.bigtable.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;

public class TableStoreNamespacesIT extends AbstractTableStoreTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testNamespaceCRUD() throws ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	SchemaManager schemaManager = storageEngine.getSchemaManager();

	// Check that only system namespace is available.
	Iterable<NamespaceDescriptor> namespaces = schemaManager.getNamespaces();
	Iterator<NamespaceDescriptor> namespaceIterator = namespaces.iterator();
	assertTrue(namespaceIterator.hasNext());
	assertEquals("system", namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	Iterable<NamespaceDefinition> namespacesIterable = ddl.getNamespaces();
	Iterator<NamespaceDefinition> iterator = namespacesIterable.iterator();
	assertTrue(iterator.hasNext());
	assertEquals("system", iterator.next().getName());
	assertFalse(iterator.hasNext());

	CreateNamespace createNamespace = ddl.createCreateNamespace("namespacesit");
	createNamespace.execute();

	namespaces = schemaManager.getNamespaces();
	namespaceIterator = namespaces.iterator();
	assertTrue(namespaceIterator.hasNext());
	assertEquals("system", namespaceIterator.next().getName());
	assertTrue(namespaceIterator.hasNext());
	assertEquals("namespacesit", namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());

	namespacesIterable = ddl.getNamespaces();
	iterator = namespacesIterable.iterator();
	assertTrue(iterator.hasNext());
	assertEquals("system", iterator.next().getName());
	assertTrue(iterator.hasNext());
	assertEquals("namespacesit", iterator.next().getName());
	assertFalse(iterator.hasNext());

	DropNamespace dropNamespace = ddl.createDropNamespace("namespacesit");
	dropNamespace.execute();

	namespaces = schemaManager.getNamespaces();
	namespaceIterator = namespaces.iterator();
	assertTrue(namespaceIterator.hasNext());
	assertEquals("system", namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());

	namespacesIterable = ddl.getNamespaces();
	iterator = namespacesIterable.iterator();
	assertTrue(iterator.hasNext());
	assertEquals("system", iterator.next().getName());
	assertFalse(iterator.hasNext());
    }

    @Test
    public void testCreationOfSystemNamespaceForbidden() throws ExecutionException {
	exception.expect(ExecutionException.class);
	exception.expectMessage("Creation of 'system' namespace is not allowed.");

	TableStoreImpl tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	ddl.createCreateNamespace("system").execute();
    }

    @Test
    public void testDroppingOfSystemNamespaceForbidden() throws ExecutionException {
	exception.expect(ExecutionException.class);
	exception.expectMessage("Dropping of 'system' namespace is not allowed.");

	TableStoreImpl tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	ddl.createDropNamespace("system").execute();
    }

}
