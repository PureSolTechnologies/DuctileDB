package com.puresoltechnologies.ductiledb.core.tables.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.api.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.NamespaceDefinition;
import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;

public class TableStoreNamespacesIT extends AbstractTableStoreTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testEmptyDatabase() {
	TableStoreImpl tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CloseableIterable<NamespaceDefinition> namespaces = ddl.getNamespaces();
	assertNotNull(namespaces);
	Iterator<NamespaceDefinition> iterator = namespaces.iterator();
	assertNotNull(iterator);
	assertFalse(iterator.hasNext());
    }

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
	// TODO check also with TableStore API!!!

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateNamespace createNamespace = ddl.createCreateNamespace("namespacesit");
	createNamespace.execute();

	namespaces = schemaManager.getNamespaces();
	namespaceIterator = namespaces.iterator();
	assertTrue(namespaceIterator.hasNext());
	assertEquals("system", namespaceIterator.next().getName());
	assertTrue(namespaceIterator.hasNext());
	assertEquals("namespacesit", namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());
	// TODO check also with TableStore API!!!

	DropNamespace dropNamespace = ddl.createDropNamespace("namespacesit");
	dropNamespace.execute();

	namespaces = schemaManager.getNamespaces();
	namespaceIterator = namespaces.iterator();
	assertTrue(namespaceIterator.hasNext());
	assertEquals("system", namespaceIterator.next().getName());
	assertFalse(namespaceIterator.hasNext());
	// TODO check also with TableStore API!!!
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
