package com.puresoltechnologies.ductiledb.engine.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.bigtable.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.bigtable.TableDescriptor;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.engine.AbstractDatabaseEngineTest;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class SchemaManagerIT extends AbstractDatabaseEngineTest {

    @Test
    public void testCreateAndDropNamespace() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	Iterator<NamespaceDescriptor> namespaces = schemaManager.getNamespaces().iterator();
	assertFalse("No namespace should be present.", namespaces.hasNext());
	assertNull("No namespace should be present.", schemaManager.getNamespace("namespace"));

	NamespaceDescriptor namespace = schemaManager.createNamespace("namespace");
	assertNotNull(namespace);

	namespaces = schemaManager.getNamespaces().iterator();
	assertTrue("Namespaces were expected to be present.", namespaces.hasNext());
	NamespaceDescriptor descriptor = namespaces.next();
	assertEquals(namespace, descriptor);
	assertEquals(namespace, schemaManager.getNamespace(namespace.getName()));

	assertFalse(namespaces.hasNext());

	schemaManager.dropNamespace(namespace);
	assertFalse("No namespace should be present.", namespaces.hasNext());
	assertNull("No namespace should be present.", schemaManager.getNamespace("namespace"));
    }

    @Test
    public void testCreateAndDropTable() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespace("namespace2");
	try {

	    Iterator<TableDescriptor> tables = schemaManager.getTables(namespace).iterator();
	    assertFalse("No table should be present.", tables.hasNext());
	    assertNull("No table should be present.", schemaManager.getTable(namespace, "table"));

	    TableDescriptor table = schemaManager.createTable(namespace, "table", "");
	    assertNotNull(table);

	    tables = schemaManager.getTables(namespace).iterator();
	    assertTrue("Tables were expected to be present.", tables.hasNext());
	    TableDescriptor descriptor = tables.next();
	    assertEquals(table, descriptor);
	    assertEquals(table, schemaManager.getTable(namespace, table.getName()));

	    assertFalse(tables.hasNext());

	    schemaManager.dropTable(table);
	    assertFalse("No table should be present.", tables.hasNext());
	    assertNull("No table should be present.", schemaManager.getNamespace("namespace"));
	} finally {
	    schemaManager.dropNamespace(namespace);
	}
    }

    @Test
    public void testCreateAndDropColumnFamily() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespace("namespace2");
	try {
	    TableDescriptor table = schemaManager.createTable(namespace, "table2", "");

	    Iterator<ColumnFamilyDescriptor> columnFamilies = schemaManager.getColumnFamilies(table).iterator();
	    assertFalse("No column family should be present.", columnFamilies.hasNext());
	    assertNull("No column family should be present.", schemaManager.getTable(namespace, "table"));

	    ColumnFamilyDescriptor columnFamily = schemaManager.createColumnFamily(table, Key.of("columnFamily"));
	    assertNotNull(columnFamily);

	    columnFamilies = schemaManager.getColumnFamilies(table).iterator();
	    assertTrue("Column families were expected to be present.", columnFamilies.hasNext());
	    ColumnFamilyDescriptor descriptor = columnFamilies.next();
	    assertEquals(columnFamily, descriptor);
	    assertEquals(columnFamily, schemaManager.getColumnFamily(table, columnFamily.getName()));

	    assertFalse(columnFamilies.hasNext());

	    schemaManager.dropColumnFamily(columnFamily);
	    assertFalse("No column family should be present.", columnFamilies.hasNext());
	    assertNull("No column family should be present.", schemaManager.getNamespace("namespace"));
	} finally {
	    schemaManager.dropNamespace(namespace);
	}
    }

    @Test(expected = SchemaException.class)
    public void testInvalidNamespaceIdentifier() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	schemaManager.createNamespace("01234567890");
    }

    @Test(expected = SchemaException.class)
    public void testInvalidTableIdentifier() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespace = schemaManager.createNamespace("validNamespace");
	try {
	    schemaManager.createTable(namespace, "0123456789", "");
	} finally {
	    schemaManager.dropNamespace(namespace);
	}
    }

}
