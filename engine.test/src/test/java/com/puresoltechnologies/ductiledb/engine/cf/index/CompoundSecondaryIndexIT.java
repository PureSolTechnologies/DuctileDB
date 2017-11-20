package com.puresoltechnologies.ductiledb.engine.cf.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Ignore;
import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyImpl;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnKeySet;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.columnfamily.index.IndexType;
import com.puresoltechnologies.ductiledb.columnfamily.index.SecondaryIndexDescriptor;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.engine.AbstractColumnFamiliyEngineTest;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class CompoundSecondaryIndexIT extends AbstractColumnFamiliyEngineTest {

    private static final String NAMESPACE = CompoundSecondaryIndexIT.class.getSimpleName();

    private static final int TEST_SIZE = 150;

    @Test
    public void testSecondaryIndexCreateGetDelete() throws IOException {
	try (ColumnFamilyImpl columnFamily = createTestColumnFamily(NAMESPACE, "testSecondaryIndexCreateGetDelete",
		"testcf")) {

	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Key.of("column1"));
	    columnKeySet.add(Key.of("column2"));
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
	    Iterator<Key> columnIterator = columns.iterator();
	    assertTrue(columnIterator.hasNext());
	    assertEquals("column1", Bytes.toString(columnIterator.next().getBytes()));
	    assertEquals("column2", Bytes.toString(columnIterator.next().getBytes()));
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
    public void testSecondaryIndexGetByHeapIndex() throws IOException {
	testIndex(IndexType.HEAP, "testSecondaryIndexGetByHeapIndex");
    }

    @Test
    @Ignore("Not implemented, yet.")
    public void testSecondaryIndexGetByClusteredIndex() throws IOException {
	testIndex(IndexType.CLUSTERED, "testSecondaryIndexGetByClusteredIndex");
    }

    private void testIndex(IndexType indexType, String tableName) throws IOException {
	try (ColumnFamilyImpl columnFamily = createTestColumnFamily(NAMESPACE, tableName, "testcf")) {
	    ColumnKeySet columnKeySet = new ColumnKeySet();
	    columnKeySet.add(Key.of("indexed1"));
	    columnKeySet.add(Key.of("indexed2"));
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
		    columnMap.put(Key.of("indexed1"), ColumnValue.of(j));
		    columnMap.put(Key.of("indexed2"), ColumnValue.of(j * 10));
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
		    assertEquals(Bytes.toInt(row.getRowKey().getBytes()),
			    Bytes.toInt(row.getColumnMap().get(Key.of("id")).getBytes()));
		    assertEquals(i, Bytes.toInt(row.getColumnMap().get(Key.of("indexed")).getBytes()));
		}
		assertFalse(found.hasNext());
	    }
	    readingTime.stop();
	    System.out.println("Reading time for type '" + indexType.name() + "' with test size '" + TEST_SIZE + "': "
		    + readingTime.toString());
	}
    }
}
