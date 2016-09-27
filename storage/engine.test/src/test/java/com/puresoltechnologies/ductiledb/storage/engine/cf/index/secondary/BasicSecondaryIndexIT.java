package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineIT;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnKeySet;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class BasicSecondaryIndexIT extends AbstractColumnFamiliyEngineTest {

    private static final String NAMESPACE = DatabaseEngineIT.class.getSimpleName();

    private static final int TEST_SIZE = 150;

    @Test
    public void testSecondaryIndexCreateGetDelete() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamily = createTestColumnFamily(NAMESPACE,
		"testSecondaryIndexCreateGetDelete", "testcf")) {

	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Bytes.toBytes("column1"));
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
	ColumnFamilyEngineImpl columnFamily = tableEngine.getColumnFamilyEngine(Bytes.toBytes("testcf"));

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
	    columnKeySet.add(Bytes.toBytes("indexed"));
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
		    columnMap.put(Bytes.toBytes("id"), Bytes.toBytes(id));
		    columnMap.put(Bytes.toBytes("indexed"), Bytes.toBytes(j));
		    columnFamily.put(Bytes.toBytes(id), columnMap);
		}
	    }
	    writingTime.stop();
	    System.out.println("Writing time for type '" + indexType.name() + "' with test size '" + TEST_SIZE + "': "
		    + writingTime.toString());
	    StopWatch readingTime = new StopWatch();
	    readingTime.start();
	    for (int i = 0; i < TEST_SIZE; ++i) {
		ColumnFamilyScanner found = columnFamily.find(Bytes.toBytes("indexed"), Bytes.toBytes(i));
		for (int j = 0; j <= i; ++j) {
		    assertTrue(found.hasNext());
		    ColumnFamilyRow row = found.next();
		    assertEquals(Bytes.toInt(row.getRowKey().getKey()),
			    Bytes.toInt(row.getColumnMap().get(Bytes.toBytes("id")).getValue()));
		    assertEquals(i, Bytes.toInt(row.getColumnMap().get(Bytes.toBytes("indexed")).getValue()));
		}
		assertFalse(found.hasNext());
	    }
	    readingTime.stop();
	    System.out.println("Reading time for type '" + indexType.name() + "' with test size '" + TEST_SIZE + "': "
		    + readingTime.toString());
	}

    }
}