package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Ignore;
import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnKeySet;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class CompoundSecondaryIndexIT extends AbstractColumnFamiliyEngineTest {

    private static final String NAMESPACE = CompoundSecondaryIndexIT.class.getSimpleName();

    private static final int TEST_SIZE = 150;

    @Test
    public void testSecondaryIndexCreateGetDelete() throws SchemaException, StorageException {
	try (ColumnFamilyEngineImpl columnFamily = createTestColumnFamily(NAMESPACE,
		"testSecondaryIndexCreateGetDelete", "testcf")) {

	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Bytes.toBytes("column1"));
	    columnKeySet.add(Bytes.toBytes("column2"));
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
	    assertEquals(2, columns.size());
	    Iterator<byte[]> columnIterator = columns.iterator();
	    assertTrue(columnIterator.hasNext());
	    assertEquals("column1", Bytes.toString(columnIterator.next()));
	    assertEquals("column2", Bytes.toString(columnIterator.next()));
	    assertFalse(columnIterator.hasNext());

	    columnFamily.dropIndex("IDX_TEST");

	    indizes = columnFamily.getIndizes();
	    iterator = indizes.iterator();
	    assertFalse(iterator.hasNext());

	    index = columnFamily.getIndex("IDX_TEST");
	    assertNull(index);
	}
    }

    @Test
    @Ignore("Not implemented, yet.")
    public void testSecondaryIndexGetByHeapIndex() throws SchemaException, StorageException {
	testIndex(IndexType.HEAP, "testSecondaryIndexGetByHeapIndex");
    }

    @Test
    @Ignore("Not implemented, yet.")
    public void testSecondaryIndexGetByClusteredIndex() throws SchemaException, StorageException {
	testIndex(IndexType.CLUSTERED, "testSecondaryIndexGetByClusteredIndex");
    }

    private void testIndex(IndexType indexType, String tableName) throws StorageException, SchemaException {
	try (ColumnFamilyEngineImpl columnFamily = createTestColumnFamily(NAMESPACE, tableName, "testcf")) {
	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Bytes.toBytes("indexed1"));
	    columnKeySet.add(Bytes.toBytes("indexed2"));
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
		    columnMap.put(Bytes.toBytes("indexed1"), Bytes.toBytes(j));
		    columnMap.put(Bytes.toBytes("indexed2"), Bytes.toBytes(j * 10));
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
