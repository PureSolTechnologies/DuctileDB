package com.puresoltechnologies.ductiledb.storage.engine.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.engine.AbstractStorageEngineTest;
import com.puresoltechnologies.ductiledb.storage.engine.StorageEngine;

public class SchemaManagerIT extends AbstractStorageEngineTest {

    @Test
    public void testCreateAndDropNamespace() throws SchemaException {
	StorageEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	Iterator<NamespaceDescriptor> namespaces = schemaManager.getNamespaces();
	assertFalse("No namespace should be present.", namespaces.hasNext());

	NamespaceDescriptor namespace = schemaManager.createNamespace("namespace");
	assertNotNull(namespace);

	namespaces = schemaManager.getNamespaces();
	assertTrue("Namespaces were expected to be present.", namespaces.hasNext());
	NamespaceDescriptor descriptor = namespaces.next();
	assertEquals(namespace, descriptor);

	assertFalse(namespaces.hasNext());
    }
}
