package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.bigtable.Get;
import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.bigtable.Result;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class DatabaseEngineIT extends AbstractDatabaseEngineTest {

    private static final String NAMESPACE = DatabaseEngineIT.class.getSimpleName();

    @Test
    public void testSimpleCRUD() throws IOException {
	DatabaseEngine engine = getEngine();

	Namespace namespace = engine.addNamespace(NAMESPACE);
	BigTable table = namespace.addTable("testSimpleCRUD", "");
	ColumnFamily columnFamily = table.addColumnFamily(Key.of("testcf"));

	Key key = Key.of(new byte[] { 1 });
	Put put = new Put(key);
	put.addColumn(columnFamily.getName(), Key.of(2), ColumnValue.of(3));
	table.put(put);

	Result result = table.get(new Get(key));
	assertNotNull(result);
	assertFalse(result.isEmpty());
	Set<Key> families = result.getFamilies();
	assertNotNull(families);
	assertFalse(families.isEmpty());
	ColumnMap familyMap = result.getFamilyMap(columnFamily.getName());
	assertEquals(ColumnValue.of(3), familyMap.get(Key.of(2)));
    }

}
