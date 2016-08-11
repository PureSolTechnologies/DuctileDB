package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Set;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class DatabaseEngineIT extends AbstractDatabaseEngineTest {

    @Test
    public void test() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent("namespace_test");
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription, "table_test");
	ColumnFamilyDescriptor columnFamily = schemaManager.createColumnFamilyIfNotPresent(tableDescription,
		Bytes.toBytes("columnfamily_test"));
	Table table = engine.getTable(tableDescription);

	byte[] key = new byte[] { 1 };
	Put put = new Put(key);
	put.addColumn(columnFamily.getName(), new byte[] { 2 }, new byte[] { 3 });
	table.put(put);

	Result result = table.get(new Get(key));
	assertNotNull(result);
	assertFalse(result.isEmpty());
	Set<byte[]> families = result.getFamilies();
	assertNotNull(families);
	assertFalse(families.isEmpty());

    }

}
