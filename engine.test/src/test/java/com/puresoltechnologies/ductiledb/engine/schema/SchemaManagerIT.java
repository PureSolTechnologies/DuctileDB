package com.puresoltechnologies.ductiledb.engine.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.engine.AbstractDatabaseEngineTest;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.Namespace;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class SchemaManagerIT extends AbstractDatabaseEngineTest {

    @Test
    public void testCreateAndDropNamespace() throws IOException {
	DatabaseEngine engine = getEngine();
	Iterator<String> namespaces = engine.getNamespaces().iterator();
	assertFalse("No namespace should be present.", namespaces.hasNext());
	assertNull("No namespace should be present.", engine.getNamespace("namespace"));

	Namespace namespace = engine.addNamespace("namespace");
	assertNotNull(namespace);

	namespaces = engine.getNamespaces().iterator();
	assertTrue("Namespaces were expected to be present.", namespaces.hasNext());
	String namespaceName = namespaces.next();
	assertEquals("namespace", namespaceName);
	assertEquals(namespace, engine.getNamespace(namespaceName));

	assertFalse(namespaces.hasNext());

	engine.dropNamespace("namespace");
	assertFalse("No namespace should be present.", namespaces.hasNext());
	assertNull("No namespace should be present.", engine.getNamespace("namespace"));
    }

    @Test
    public void testCreateAndDropTable() throws IOException {
	DatabaseEngine engine = getEngine();
	Namespace namespace = engine.addNamespace("namespace2");
	try {

	    Iterator<String> tables = namespace.getTables().iterator();
	    assertFalse("No table should be present.", tables.hasNext());
	    assertNull("No table should be present.", namespace.getTable("table"));

	    BigTable table = namespace.addTable("table", "");
	    assertNotNull(table);

	    tables = namespace.getTables().iterator();
	    assertTrue("Tables were expected to be present.", tables.hasNext());
	    String tableName = tables.next();
	    assertEquals("table", tableName);
	    assertEquals(table, namespace.getTable("table"));

	    assertFalse(tables.hasNext());

	    namespace.dropTable("table");
	    assertFalse("No table should be present.", tables.hasNext());
	    assertNull("No table should be present.", namespace.getTable("table"));
	} finally {
	    engine.dropNamespace("namespace2");
	}
    }

    @Test
    public void testCreateAndDropColumnFamily() throws IOException {
	DatabaseEngine engine = getEngine();
	Namespace namespace = engine.addNamespace("namespace3");
	try {
	    BigTable table = namespace.addTable("table2", "");

	    Key cfId = Key.of("columnFamily");
	    Iterator<Key> columnFamilies = table.getColumnFamilies().iterator();
	    assertFalse("No column family should be present.", columnFamilies.hasNext());
	    assertNull("No column family should be present.", table.getColumnFamily(cfId));

	    ColumnFamily columnFamily = table.addColumnFamily(cfId);
	    assertNotNull(columnFamily);

	    columnFamilies = table.getColumnFamilies().iterator();
	    assertTrue("Column families were expected to be present.", columnFamilies.hasNext());
	    Key key = columnFamilies.next();
	    assertEquals(cfId, key);
	    assertEquals(columnFamily, table.getColumnFamily(cfId));

	    assertFalse(columnFamilies.hasNext());

	    table.dropColumnFamily(cfId);
	    assertFalse("No column family should be present.", columnFamilies.hasNext());
	    assertNull("No column family should be present.", table.getColumnFamily(cfId));
	} finally {
	    engine.dropNamespace("namespace3");
	}
    }

}
