package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class DatabaseEngineIT extends AbstractStorageEngineTest {

    @Test
    public void test() throws SchemaException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespace("engine_test");
	TableDescriptor tableDescription = schemaManager.createTable(namespaceDescription, "puttest");
	ColumnFamilyDescriptor columnFamily = schemaManager.createColumnFamily(tableDescription, "puts");
	Table table = engine.getTable(tableDescription);

	byte[] key = new byte[] { 1 };
	Put put = new Put(key);
	put.addColumn(columnFamily.getName(), new byte[] { 2 }, new byte[] { 3 });
	table.put(put);

	Result result = table.get(new Get(key));
	assertNotNull(result);

    }

}
