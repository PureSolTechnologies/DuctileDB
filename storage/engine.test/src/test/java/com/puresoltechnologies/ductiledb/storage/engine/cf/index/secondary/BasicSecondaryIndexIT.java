package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineIT;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnKeySet;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class BasicSecondaryIndexIT extends AbstractColumnFamiliyEngineTest {

    private static final String NAMESPACE = DatabaseEngineIT.class.getSimpleName();

    @Test
    public void testSecondaryIndexCreateGetDelete() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamily = createTestColumnFamily(NAMESPACE,
		"testSecondaryIndexCreateGetDelete", "testcf")) {

	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Bytes.toBytes("column1"));
	    SecondaryIndexDescriptor indexDescriptor = new SecondaryIndexDescriptor("IDX_TEST",
		    columnFamily.getDescriptor(), columnKeySet);

	    Iterable<SecondaryIndexDescriptor> indizes = columnFamily.getIndizes();
	    Iterator<SecondaryIndexDescriptor> iterator = indizes.iterator();
	    assertFalse(iterator.hasNext());

	    SecondaryIndexDescriptor index = columnFamily.getIndex("IDX_TEST");
	    assertNull(index);

	    columnFamily.createIndex(indexDescriptor);

	    indizes = columnFamily.getIndizes();
	    iterator = indizes.iterator();
	    assertTrue(iterator.hasNext());
	    assertEquals(indexDescriptor, iterator.next());
	    assertFalse(iterator.hasNext());

	    index = columnFamily.getIndex("IDX_TEST");
	    assertEquals(indexDescriptor, index);
	    ColumnKeySet columns = index.getColumns();
	    assertEquals(1, columns.size());
	    assertEquals("column1", Bytes.toString(columns.iterator().next()));

	    columnFamily.dropIndex("IDX_TEST");

	    indizes = columnFamily.getIndizes();
	    iterator = indizes.iterator();
	    assertFalse(iterator.hasNext());

	    index = columnFamily.getIndex("IDX_TEST");
	    assertNull(index);
	}
    }

    @Test
    public void testSecondaryIndexSurvivesRestart() throws SchemaException, StorageException, IOException {
	SecondaryIndexDescriptor indexDescriptor;
	try (ColumnFamilyEngineImpl columnFamily = createTestColumnFamily(NAMESPACE,
		"testSecondaryIndexSurvivesRestart", "testcf")) {
	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Bytes.toBytes("testcol"));
	    indexDescriptor = new SecondaryIndexDescriptor("IDX_TEST", columnFamily.getDescriptor(), columnKeySet);
	    columnFamily.createIndex(indexDescriptor);

	    Iterable<SecondaryIndexDescriptor> indizes = columnFamily.getIndizes();
	    Iterator<SecondaryIndexDescriptor> iterator = indizes.iterator();
	    assertTrue(iterator.hasNext());
	    assertEquals(indexDescriptor, iterator.next());
	    assertFalse(iterator.hasNext());
	}

	stopEngine();
	startEngine();

	DatabaseEngineImpl engine = getEngine();
	NamespaceEngineImpl namespaceEngine = engine.getNamespaceEngine(NAMESPACE);
	TableEngineImpl tableEngine = namespaceEngine.getTableEngine("testSecondaryIndexSurvivesRestart");
	ColumnFamilyEngineImpl columnFamily = tableEngine.getColumnFamilyEngine(Bytes.toBytes("testcf"));

	Iterable<SecondaryIndexDescriptor> indizes = columnFamily.getIndizes();
	Iterator<SecondaryIndexDescriptor> iterator = indizes.iterator();
	assertTrue(iterator.hasNext());
	assertEquals(indexDescriptor, iterator.next());
	assertFalse(iterator.hasNext());
    }

    @Test
    public void testSecondaryIndexGetByIndex() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamily = createTestColumnFamily(NAMESPACE, "testSecondaryIndexGetByIndex",
		"testcf")) {
	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Bytes.toBytes("testcol"));
	    SecondaryIndexDescriptor indexDescriptor = new SecondaryIndexDescriptor("IDX_TEST",
		    columnFamily.getDescriptor(), columnKeySet);
	    columnFamily.createIndex(indexDescriptor);

	    Iterable<SecondaryIndexDescriptor> indizes = columnFamily.getIndizes();
	    Iterator<SecondaryIndexDescriptor> iterator = indizes.iterator();
	    assertTrue(iterator.hasNext());
	    assertEquals(indexDescriptor, iterator.next());
	    assertFalse(iterator.hasNext());
	}
    }
}