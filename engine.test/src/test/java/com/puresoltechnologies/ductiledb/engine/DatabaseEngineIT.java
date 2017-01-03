package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Set;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.bigtable.Get;
import com.puresoltechnologies.ductiledb.bigtable.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.bigtable.Result;
import com.puresoltechnologies.ductiledb.bigtable.TableDescriptor;
import com.puresoltechnologies.ductiledb.bigtable.TableEngine;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class DatabaseEngineIT extends AbstractDatabaseEngineTest {

    private static final String NAMESPACE = DatabaseEngineIT.class.getSimpleName();

    @Test
    public void testSimpleCRUD() throws SchemaException, StorageException {
	DatabaseEngine engine = getEngine();
	SchemaManager schemaManager = engine.getSchemaManager();
	NamespaceDescriptor namespaceDescription = schemaManager.createNamespaceIfNotPresent(NAMESPACE);
	TableDescriptor tableDescription = schemaManager.createTableIfNotPresent(namespaceDescription, "testSimpleCRUD",
		"");
	ColumnFamilyDescriptor columnFamily = schemaManager.createColumnFamilyIfNotPresent(tableDescription,
		Key.of("testcf"));
	TableEngine table = engine.getTable(tableDescription);

	Key key = Key.of(new byte[] { 1 });
	Put put = new Put(key);
	put.addColumn(columnFamily.getName(), Key.of(new byte[] { 2 }), ColumnValue.of(new byte[] { 3 }));
	table.put(put);

	Result result = table.get(new Get(key));
	assertNotNull(result);
	assertFalse(result.isEmpty());
	Set<Key> families = result.getFamilies();
	assertNotNull(families);
	assertFalse(families.isEmpty());
    }

}
