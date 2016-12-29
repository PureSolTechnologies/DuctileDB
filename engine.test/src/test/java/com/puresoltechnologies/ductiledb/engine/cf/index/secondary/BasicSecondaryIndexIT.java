package com.puresoltechnologies.ductiledb.engine.cf.index.secondary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.engine.TableEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnKeySet;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class BasicSecondaryIndexIT extends AbstractColumnFamiliyEngineTest {

    private static final String NAMESPACE = BasicSecondaryIndexIT.class.getSimpleName();

    private static final int TEST_SIZE = 150;

    @Test
    public void testSecondaryIndexCreateGetDelete() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamily = createTestColumnFamily(NAMESPACE,
		"testSecondaryIndexCreateGetDelete", "testcf")) {

	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Key.of("column1"));
	    SecondaryIndexDescriptor indexDescriptor = new SecondaryIndexDescriptor("IDX_TEST",
		    columnFamily.getDescriptor(), columnKeySet, IndexType.HEAP);

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
	    assertEquals("column1", columns.iterator().next().toString());

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
	    columnKeySet.add(Key.of("testcol"));
	    indexDescriptor = new SecondaryIndexDescriptor("IDX_TEST", columnFamily.getDescriptor(), columnKeySet,
		    IndexType.HEAP);
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
	ColumnFamilyEngineImpl columnFamily = tableEngine.getColumnFamilyEngine(Key.of("testcf"));

	Iterable<SecondaryIndexDescriptor> indizes = columnFamily.getIndizes();
	Iterator<SecondaryIndexDescriptor> iterator = indizes.iterator();
	assertTrue(iterator.hasNext());
	assertEquals(indexDescriptor, iterator.next());
	assertFalse(iterator.hasNext());
    }

    @Test
    public void testSecondaryIndexGetByHeapIndex() throws SchemaException, StorageException {
	testIndex(IndexType.HEAP, "testSecondaryIndexGetByHeapIndex");
    }

    @Test
    public void testSecondaryIndexGetByClusteredIndex() throws SchemaException, StorageException {
	testIndex(IndexType.CLUSTERED, "testSecondaryIndexGetByClusteredIndex");
    }

    private void testIndex(IndexType indexType, String tableName) throws StorageException, SchemaException {
	try (ColumnFamilyEngineImpl columnFamily = createTestColumnFamily(NAMESPACE, tableName, "testcf")) {
	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Key.of("indexed"));
	    SecondaryIndexDescriptor indexDescriptor = new SecondaryIndexDescriptor("IDX_TEST",
		    columnFamily.getDescriptor(), columnKeySet, indexType);
	    columnFamily.createIndex(indexDescriptor);

	    Iterable<SecondaryIndexDescriptor> indizes = columnFamily.getIndizes();
	    Iterator<SecondaryIndexDescriptor> iterator = indizes.iterator();
	    assertTrue(iterator.hasNext());
	    assertEquals(indexDescriptor, iterator.next());
	    assertFalse(iterator.hasNext());

	    StopWatch writingTime = new StopWatch();
	    writingTime.start();
	    ColumnMap columnMap = new ColumnMap();
	    for (int i = 0; i < TEST_SIZE; ++i) {
		for (int j = i; j < TEST_SIZE; ++j) {
		    int id = i * TEST_SIZE + j;
		    columnMap.put(Key.of("id"), ColumnValue.of(id));
		    columnMap.put(Key.of("indexed"), ColumnValue.of(j));
		    columnFamily.put(Key.of(id), columnMap);
		}
	    }
	    writingTime.stop();
	    System.out.println("Writing time for type '" + indexType.name() + "' with test size '" + TEST_SIZE + "': "
		    + writingTime.toString());
	    StopWatch readingTime = new StopWatch();
	    readingTime.start();
	    for (int i = 0; i < TEST_SIZE; ++i) {
		ColumnFamilyScanner found = columnFamily.find(Key.of("indexed"), ColumnValue.of(i));
		for (int j = 0; j <= i; ++j) {
		    assertTrue(found.hasNext());
		    ColumnFamilyRow row = found.next();
		    assertEquals(row.getRowKey().toInt(), row.getColumnMap().get(Key.of("id")).toInt());
		    assertEquals(i, row.getColumnMap().get(Key.of("indexed")).toInt());
		}
		assertFalse(found.hasNext());
	    }
	    readingTime.stop();
	    System.out.println("Reading time for type '" + indexType.name() + "' with test size '" + TEST_SIZE + "': "
		    + readingTime.toString());
	}

    }
}