package com.puresoltechnologies.ductiledb.engine.cf.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.bigtable.BigTableImpl;
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
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.engine.NamespaceImpl;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class BasicSecondaryIndexIT extends AbstractColumnFamiliyEngineTest {

    private static final String NAMESPACE = BasicSecondaryIndexIT.class.getSimpleName();

    private static final int TEST_SIZE = 150;

    @Test
    public void testSecondaryIndexCreateGetDelete() throws IOException {
	try (ColumnFamilyImpl columnFamily = createTestColumnFamily(NAMESPACE, "testSecondaryIndexCreateGetDelete",
		"testcf")) {

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
	    assertEquals("column1", Bytes.toString(columns.iterator().next().getBytes()));

	    columnFamily.dropIndex("IDX_TEST");

	    indizes = columnFamily.getIndizes();
	    iterator = indizes.iterator();
	    assertFalse(iterator.hasNext());

	    index = columnFamily.getIndex("IDX_TEST");
	    assertNull(index);
	}
    }

    @Test
    public void testSecondaryIndexSurvivesRestart() throws IOException {
	SecondaryIndexDescriptor indexDescriptor;
	try (ColumnFamilyImpl columnFamily = createTestColumnFamily(NAMESPACE, "testSecondaryIndexSurvivesRestart",
		"testcf")) {
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
	NamespaceImpl namespaceEngine = (NamespaceImpl) engine.getNamespace(NAMESPACE);
	BigTableImpl tableEngine = (BigTableImpl) namespaceEngine.getTable("testSecondaryIndexSurvivesRestart");
	ColumnFamilyImpl columnFamily = (ColumnFamilyImpl) tableEngine.getColumnFamilyEngine(Key.of("testcf"));

	Iterable<SecondaryIndexDescriptor> indizes = columnFamily.getIndizes();
	Iterator<SecondaryIndexDescriptor> iterator = indizes.iterator();
	assertTrue(iterator.hasNext());
	assertEquals(indexDescriptor, iterator.next());
	assertFalse(iterator.hasNext());
    }

    @Test
    public void testSecondaryIndexGetByHeapIndex() throws IOException {
	testIndex(IndexType.HEAP, "testSecondaryIndexGetByHeapIndex");
    }

    @Test
    public void testSecondaryIndexGetByClusteredIndex() throws IOException {
	testIndex(IndexType.CLUSTERED, "testSecondaryIndexGetByClusteredIndex");
    }

    private void testIndex(IndexType indexType, String tableName) throws IOException {
	try (ColumnFamilyImpl columnFamily = createTestColumnFamily(NAMESPACE, tableName, "testcf")) {
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
		    assertEquals(row.getRowKey().toIntValue(), row.getColumnMap().get(Key.of("id")).toInt());
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